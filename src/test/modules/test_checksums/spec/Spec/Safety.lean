/-
  Safety of the data checksum state machine.

  Two properties are proved for every reachable cluster state:

    writeSafe  If any process verifies checksums, every process writes them.
    readSafe   If any process verifies checksums, every page on disk has one,
               i.e. `PageIsVerified()` never reports a spurious checksum
               failure.

  Both are properties of the *readers* (`DataChecksumsNeedVerify()` and
  `DataChecksumsNeedWrite()`); everything else in this development exists to
  make them come out true.
-/
import Spec.Cluster

namespace Spec

open ChecksumState Cluster

/-! ## The table audit

These are finite, decidable facts about `checksum_barriers` alone.  They are
what a reviewer should look at first when the table in `datachecksum_state.c`
is edited.
-/

/-- No entry of `checksum_barriers` pairs a verifying backend with a
non-writing one. -/
theorem barrierTable_mixSafe :
    barrierTable.all (fun e => mixSafe e.1 e.2) = true := by decide

/-- In particular the table contains no direct `on`/`off` edge in either
direction; that is the entire reason the two `inprogress-*` states exist. -/
theorem barrierTable_no_direct_toggle :
    barrierTable.contains (.on, .off) = false
    ∧ barrierTable.contains (.off, .on) = false := by decide

/-- Only a writing state can lead into `on`. -/
theorem table_into_on {s : ChecksumState}
    (h : barrierTable.contains (s, .on) = true) : needWrite s = true := by
  cases s <;> revert h <;> decide

/-- Only a writing state can be reached from `on`. -/
theorem table_from_on {t : ChecksumState}
    (h : barrierTable.contains (.on, t) = true) : needWrite t = true := by
  cases t <;> revert h <;> decide

/--
The core local argument, as a decidable check over all `4^3` combinations.

Read it as: while a barrier for `g` is in flight, the processes are split
between `g` and the single state they all shared beforehand.  Whichever way
you pick two of them, if one verifies then the other writes.
-/
def pairwiseSafeIn (tbl : List (ChecksumState × ChecksumState)) : Bool :=
  ChecksumState.all.all fun a =>
    ChecksumState.all.all fun b =>
      ChecksumState.all.all fun g =>
        !(absorbAllowedIn tbl a g && absorbAllowedIn tbl b g
            && (a == g || b == g) && needVerify a)
          || needWrite b

/-- `checksum_barriers` as it stands passes the check. -/
theorem barrierTable_pairwiseSafe : pairwiseSafeIn barrierTable = true := by decide

/-! ### Negative results

The three theorems below are the reason the check has teeth: they show that
the obvious "simplifications" of the design break it.  If someone adds a
shortcut edge to `checksum_barriers`, `barrierTable_pairwiseSafe` stops
type-checking.
-/

/-- Allowing `on -> off` directly is unsafe: a backend that has not yet
absorbed the barrier still verifies, while one that has already absorbed it
has stopped writing. -/
theorem onOff_shortcut_unsafe :
    pairwiseSafeIn ((.on, .off) :: barrierTable) = false := by decide

/-- Allowing `off -> on` directly is unsafe for the mirror-image reason. -/
theorem offOn_shortcut_unsafe :
    pairwiseSafeIn ((.off, .on) :: barrierTable) = false := by decide

/-- A design with no intermediate states at all is unsafe in both
directions. -/
theorem twoStateDesign_unsafe :
    pairwiseSafeIn [(.off, .on), (.on, .off)] = false := by decide

/--
The propositional form of `barrierTable_pairwiseSafe`, used in the proofs
below.

`hlag` is the invariant that the processes only ever straddle a single edge:
at least one of the two is already at the current shared state.
-/
theorem pairwise_safe {a b g : ChecksumState}
    (ha : absorbAllowed a g = true) (hb : absorbAllowed b g = true)
    (hlag : a = g ∨ b = g) (hv : needVerify a = true) : needWrite b = true := by
  cases a <;> cases b <;> cases g <;> revert ha hb hlag hv <;> decide

/-! ## The inductive invariant -/

/--
The invariant carried along every execution.

The first two conjuncts are about *agreement* between processes, the rest tie
the shared and durable copies of the state to what is actually on disk.
-/
structure Inv {Backend Page : Type} (c : Cluster Backend Page) : Prop where
  /-- Every process is either at the shared state or one legal barrier edge
  behind it.  Established because a process only ever assigns itself
  `shared` (`InitLocalDataChecksumState()`, `AbsorbDataChecksumsBarrier()`). -/
  localCompat : ∀ b, absorbAllowed (c.localState b) c.shared = true
  /-- Of any two processes, at least one has caught up with `shared`, or they
  agree with each other.  Established by the `WaitForProcSignalBarrier()` at
  the end of every writer: a new edge is only started from a quiescent
  cluster, so the laggards all sit on the same previous state. -/
  atMostTwo : ∀ b₁ b₂, c.localState b₁ = c.shared ∨ c.localState b₂ = c.shared
                        ∨ c.localState b₁ = c.localState b₂
  /-- Once the cluster is `on`, nobody may skip writing checksums. -/
  sharedOnWrites : c.shared = .on → ∀ b, needWrite (c.localState b) = true
  /-- Once the cluster is `on`, every page carries a checksum. -/
  sharedOnPages : c.shared = .on → AllChecksummed c
  /-- While `pg_control` says `on`, the shared state is still a writing one.
  This is what bounds the window opened by updating the control file before
  waiting for the barrier. -/
  controlOnShared : c.control = .on → needWrite c.shared = true
  /-- While `pg_control` says `on`, nobody skips writing checksums, so a
  crash cannot leave an unchecksummed page behind. -/
  controlOnWrites : c.control = .on → ∀ b, needWrite (c.localState b) = true
  /-- While `pg_control` says `on`, every page carries a checksum.  This is
  what makes coming back up in `on` after an immediate shutdown safe. -/
  controlOnPages : c.control = .on → AllChecksummed c
  /-- The user-visible read property, carried inductively. -/
  readSafe : ∀ b p, needVerify (c.localState b) = true → c.onDisk p = true

namespace Inv

variable {Backend Page : Type} {c : Cluster Backend Page}

/--
`writeSafe` is a consequence of process agreement alone: no page is ever
written without a checksum while somebody might verify it.

This is the formal statement of the first correctness rule in the
`datachecksum_state.c` header comment.
-/
theorem writeSafe (h : Inv c) (b₁ b₂ : Backend)
    (hv : needVerify (c.localState b₁) = true) :
    needWrite (c.localState b₂) = true := by
  rcases h.atMostTwo b₁ b₂ with h₁ | h₁ | h₁
  · exact pairwise_safe (h.localCompat b₁) (h.localCompat b₂) (Or.inl h₁) hv
  · exact pairwise_safe (h.localCompat b₁) (h.localCompat b₂) (Or.inr h₁) hv
  · exact h₁ ▸ needVerify_le_needWrite hv

end Inv

/-! ## Preservation -/

variable {Backend Page : Type} [DecidableEq Backend] [DecidableEq Page]

omit [DecidableEq Page] in
/--
A process refreshing its local state from shared memory preserves the
invariant.  This single lemma covers both `InitLocalDataChecksumState()` and
`AbsorbDataChecksumsBarrier()`: the legality check on the edge is what keeps
`localCompat` true for *other* processes, and is not needed here because the
process ends up exactly at `shared`.
-/
theorem inv_setLocal {c : Cluster Backend Page} (h : Inv c) (b₀ : Backend) :
    Inv { c with localState := upd c.localState b₀ c.shared } := by
  have key : ∀ b, upd c.localState b₀ c.shared b = c.shared
                  ∨ upd c.localState b₀ c.shared b = c.localState b :=
    fun b => upd_cases c.localState b₀ c.shared b
  refine ⟨?_, ?_, ?_, h.sharedOnPages, h.controlOnShared, ?_, h.controlOnPages, ?_⟩
  · intro b
    show absorbAllowed (upd c.localState b₀ c.shared b) c.shared = true
    rcases key b with hb | hb <;> rw [hb]
    · exact absorbAllowed_refl _
    · exact h.localCompat b
  · intro b₁ b₂
    show upd c.localState b₀ c.shared b₁ = c.shared
         ∨ upd c.localState b₀ c.shared b₂ = c.shared
         ∨ upd c.localState b₀ c.shared b₁ = upd c.localState b₀ c.shared b₂
    by_cases hb₁ : b₁ = b₀
    · exact Or.inl (by rw [hb₁]; exact upd_same _ _ _)
    · by_cases hb₂ : b₂ = b₀
      · exact Or.inr (Or.inl (by rw [hb₂]; exact upd_same _ _ _))
      · rw [upd_other c.localState c.shared hb₁, upd_other c.localState c.shared hb₂]
        exact h.atMostTwo b₁ b₂
  · intro hon b
    have hon' : c.shared = ChecksumState.on := hon
    show needWrite (upd c.localState b₀ c.shared b) = true
    rcases key b with hb | hb <;> rw [hb]
    · rw [hon']; rfl
    · exact h.sharedOnWrites hon' b
  · intro hon b
    show needWrite (upd c.localState b₀ c.shared b) = true
    rcases key b with hb | hb <;> rw [hb]
    · exact h.controlOnShared hon
    · exact h.controlOnWrites hon b
  · intro b p hv
    have hv' : needVerify (upd c.localState b₀ c.shared b) = true := hv
    rcases key b with hb | hb <;> rw [hb] at hv'
    · exact h.sharedOnPages (needVerify_eq_on hv') p
    · exact h.readSafe b p hv'

/-- Every step of the cluster preserves the invariant. -/
theorem Inv.step {c c' : Cluster Backend Page} (h : Inv c) (hs : Step c c') : Inv c' := by
  cases hs with
  | backendStart b₀ => exact inv_setLocal h b₀
  | absorb b₀ _ _ => exact inv_setLocal h b₀
  | writePage b₀ p₀ =>
      have key : ∀ p, upd c.onDisk p₀ (needWrite (c.localState b₀)) p
                        = needWrite (c.localState b₀)
                      ∨ upd c.onDisk p₀ (needWrite (c.localState b₀)) p = c.onDisk p :=
        fun p => upd_cases c.onDisk p₀ _ p
      refine ⟨h.localCompat, h.atMostTwo, h.sharedOnWrites, ?_, h.controlOnShared,
              h.controlOnWrites, ?_, ?_⟩
      · intro hon p
        show upd c.onDisk p₀ (needWrite (c.localState b₀)) p = true
        rcases key p with hp | hp <;> rw [hp]
        · exact h.sharedOnWrites hon b₀
        · exact h.sharedOnPages hon p
      · intro hon p
        show upd c.onDisk p₀ (needWrite (c.localState b₀)) p = true
        rcases key p with hp | hp <;> rw [hp]
        · exact h.controlOnWrites hon b₀
        · exact h.controlOnPages hon p
      · intro b p hv
        show upd c.onDisk p₀ (needWrite (c.localState b₀)) p = true
        rcases key p with hp | hp <;> rw [hp]
        · exact h.writeSafe b b₀ hv
        · exact h.readSafe b p hv
  | advanceShared t hq hemit hedge hsweep =>
      have hq' : ∀ b, c.localState b = c.shared := hq
      have hemit' : c.control = c.shared := hemit
      refine ⟨?_, ?_, ?_, ?_, ?_, h.controlOnWrites, h.controlOnPages, h.readSafe⟩
      · intro b
        show absorbAllowed (c.localState b) t = true
        simp only [absorbAllowed, absorbAllowedIn, hq' b, hedge, Bool.or_true]
      · intro b₁ b₂
        exact Or.inr (Or.inr (by rw [hq' b₁, hq' b₂]))
      · intro hon b
        have hon' : t = ChecksumState.on := hon
        show needWrite (c.localState b) = true
        rw [hq' b]
        exact table_into_on (hon' ▸ hedge)
      · intro hon; exact hsweep hon
      · intro hon
        have hon' : c.control = ChecksumState.on := hon
        have hshared : c.shared = ChecksumState.on := hemit' ▸ hon'
        show needWrite t = true
        exact table_from_on (hshared ▸ hedge)
  | advanceControl =>
      refine ⟨h.localCompat, h.atMostTwo, h.sharedOnWrites, h.sharedOnPages, ?_, ?_, ?_,
              h.readSafe⟩
      · intro hon
        have hon' : c.shared = ChecksumState.on := hon
        show needWrite c.shared = true
        rw [hon']; rfl
      · intro hon; exact h.sharedOnWrites hon
      · intro hon; exact h.sharedOnPages hon
  | restart =>
      refine ⟨fun _ => absorbAllowed_refl _, fun _ _ => Or.inr (Or.inr rfl), ?_, ?_, ?_, ?_,
              ?_, ?_⟩
      · intro hon _
        have hon' : recover c.control = ChecksumState.on := hon
        show needWrite (recover c.control) = true
        rw [hon']; rfl
      · intro hon; exact h.controlOnPages (recover_eq_on hon)
      · intro hon
        have hon' : recover c.control = ChecksumState.on := hon
        show needWrite (recover c.control) = true
        rw [hon']; rfl
      · intro hon _
        have hon' : recover c.control = ChecksumState.on := hon
        show needWrite (recover c.control) = true
        rw [hon']; rfl
      · intro hon; exact h.controlOnPages (recover_eq_on hon)
      · intro _ p hv
        exact h.controlOnPages (recover_eq_on (needVerify_eq_on hv)) p

omit [DecidableEq Backend] [DecidableEq Page] in
/-- A freshly initialised cluster satisfies the invariant. -/
theorem Inv.of_init {c : Cluster Backend Page} (h : Init c) : Inv c := by
  obtain ⟨_, hctl, hloc, hpages⟩ := h
  have hs : c.control = ChecksumState.on → c.shared = ChecksumState.on := by
    intro hon; rw [← hctl]; exact hon
  refine ⟨?_, fun b₁ _ => Or.inl (hloc b₁), ?_, hpages, ?_, ?_, ?_, ?_⟩
  · intro b; rw [hloc b]; exact absorbAllowed_refl _
  · intro hon b; rw [hloc b, hon]; rfl
  · intro hon; rw [hs hon]; rfl
  · intro hon b; rw [hloc b, hs hon]; rfl
  · intro hon; exact hpages (hs hon)
  · intro b p hv
    rw [hloc b] at hv
    exact hpages (needVerify_eq_on hv) p

/-- The invariant holds in every reachable state. -/
theorem inv_of_reachable {c : Cluster Backend Page} (h : Reachable c) : Inv c := by
  induction h with
  | init hi => exact Inv.of_init hi
  | step _ hs ih => exact ih.step hs

/-! ## The two top-level results -/

/--
**Write safety.**  In any reachable state, if some process verifies data
checksums then every process is writing them.

`datachecksum_state.c`:

> all backends MUST calculate and write data checksums during all states
> except off; MUST validate checksums only in the 'on' state.
-/
theorem reachable_writeSafe {c : Cluster Backend Page} (h : Reachable c)
    (b₁ b₂ : Backend) (hv : needVerify (c.localState b₁) = true) :
    needWrite (c.localState b₂) = true :=
  (inv_of_reachable h).writeSafe b₁ b₂ hv

/--
**Read safety.**  In any reachable state, if some process verifies data
checksums then every page on disk carries one.
-/
theorem reachable_readSafe {c : Cluster Backend Page} (h : Reachable c)
    (b : Backend) (p : Page) (hv : needVerify (c.localState b) = true) :
    c.onDisk p = true :=
  (inv_of_reachable h).readSafe b p hv

/-- A checksum verification failure: `PageIsVerified()` finds
`DataChecksumsNeedVerify()` true for a page that was written without a
checksum. -/
def ChecksumFailure (c : Cluster Backend Page) (b : Backend) (p : Page) : Prop :=
  needVerify (c.localState b) = true ∧ c.onDisk p = false

/-- **No spurious checksum failures.**  The corollary the feature is really
about: online enabling and disabling never makes `PageIsVerified()` report a
checksum failure on a page the cluster itself wrote. -/
theorem no_spurious_failure {c : Cluster Backend Page} (h : Reachable c)
    (b : Backend) (p : Page) : ¬ ChecksumFailure c b p := by
  rintro ⟨hv, hp⟩
  rw [reachable_readSafe h b p hv] at hp
  exact Bool.noConfusion hp

end Spec
