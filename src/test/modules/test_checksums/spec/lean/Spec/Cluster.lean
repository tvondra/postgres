/-
  The cluster: where the checksum state lives, and how it changes.

  This file mirrors:

    src/backend/access/transam/xlog.c   XLogCtl->data_checksum_version
                                        ControlFile->data_checksum_version
                                        LocalDataChecksumState
                                        InitLocalDataChecksumState()
                                        SetLocalDataChecksumState()
                                        SetDataChecksums{OnInProgress,On,Off}()
                                        XLOGShmemInit() / StartupXLOG()
                                        xlog2_redo() / xlog_redo()
    src/backend/postmaster/datachecksum_state.c
                                        AbsorbDataChecksumsBarrier()
    src/backend/storage/page/bufpage.c  PageSetChecksum()

  See the README in this directory for the modelling assumptions.
-/
import Spec.Barriers

namespace Spec

open ChecksumState

/-- Point update of a function; used for "backend `b`'s local state" and for
"the page at `p`". -/
def upd {α β : Type} [DecidableEq α] (f : α → β) (a : α) (v : β) : α → β :=
  fun x => if x = a then v else f x

@[simp] theorem upd_same {α β : Type} [DecidableEq α] (f : α → β) (a : α) (v : β) :
    upd f a v a = v := by simp [upd]

theorem upd_other {α β : Type} [DecidableEq α] (f : α → β) {a x : α} (v : β)
    (h : x ≠ a) : upd f a v x = f x := by simp [upd, h]

/-- After a point update, every value is either the new one or the old one.
Almost every proof below only needs this much. -/
theorem upd_cases {α β : Type} [DecidableEq α] (f : α → β) (a : α) (v : β) (x : α) :
    upd f a v x = v ∨ upd f a v x = f x := by
  by_cases h : x = a
  · exact Or.inl (by simp [upd, h])
  · exact Or.inr (by simp [upd, h])

/--
A snapshot of the whole cluster.

`Backend` indexes the processes that can touch pages (regular backends,
auxiliary processes, the datachecksums worker); `Page` indexes the data pages
in the cluster.  Both are arbitrary types, so the results below hold for any
number of backends and any number of pages.
-/
structure Cluster (Backend Page : Type) where
  /-- `XLogCtl->data_checksum_version`: the authoritative cluster-wide state,
  read under `info_lck` by `DataChecksumsOn()`, `DataChecksumsOff()` and
  `DataChecksumsInProgressOn()`. -/
  shared : ChecksumState
  /-- `ControlFile->data_checksum_version`: the durable copy in `pg_control`,
  which is what survives a crash. -/
  control : ChecksumState
  /-- `LocalDataChecksumState`, one per process.  This is the only thing
  `DataChecksumsNeedWrite()` and `DataChecksumsNeedVerify()` look at. -/
  localState : Backend → ChecksumState
  /-- Whether the page currently on disk carries a valid checksum. -/
  onDisk : Page → Bool

namespace Cluster

variable {Backend Page : Type}

/-- Extensionality for cluster snapshots. -/
theorem ext : ∀ {c c' : Cluster Backend Page},
    c.shared = c'.shared → c.control = c'.control →
    (∀ b, c.localState b = c'.localState b) → (∀ p, c.onDisk p = c'.onDisk p) → c = c'
  | ⟨_, _, _, _⟩, ⟨_, _, _, _⟩, hs, hc, hl, hd => by
      simp only [Cluster.mk.injEq]
      exact ⟨hs, hc, funext hl, funext hd⟩

/-- Every process has caught up with the shared state, i.e. the last
`WaitForProcSignalBarrier()` has returned. -/
def Quiescent (c : Cluster Backend Page) : Prop :=
  ∀ b, c.localState b = c.shared

/-- The barrier for the current shared state has been emitted.  In every
writer (`SetDataChecksums*()`, `xlog2_redo()`) the control file is updated
before `EmitProcSignalBarrier()` is called, so a barrier is in flight only
once `control` has caught up with `shared`. -/
def BarrierEmitted (c : Cluster Backend Page) : Prop :=
  c.control = c.shared

/-- Every data page on disk carries a checksum.  This is what
`ProcessAllDatabases()` establishes before `SetDataChecksumsOn()` is called. -/
def AllChecksummed (c : Cluster Backend Page) : Prop :=
  ∀ p, c.onDisk p = true

end Cluster

open Cluster

/--
One atomic step of the cluster.

Reads are not steps: `DataChecksumsNeedVerify()` and friends do not change
anything, so "a read fails" is a property of a state (see `Spec.Safety`) rather
than of a transition.
-/
inductive Step {Backend Page : Type} [DecidableEq Backend] [DecidableEq Page] :
    Cluster Backend Page → Cluster Backend Page → Prop
  /--
  A process starts up and seeds its local state from shared memory:
  `InitLocalDataChecksumState()`, called from `InitPostgres()` and
  `AuxiliaryProcessMainCommon()`.

  Note this reads `shared`, not `control`, and is not gated on a barrier
  having been emitted.
  -/
  | backendStart (c : Cluster Backend Page) (b : Backend) :
      Step c { c with localState := upd c.localState b c.shared }
  /--
  A process absorbs a checksum procsignal barrier:
  `AbsorbDataChecksumsBarrier()`.

  The target state is `shared`, because every writer publishes the new value
  to `XLogCtl` before emitting the barrier that announces it.  The edge must
  be accepted by `checksum_barriers`, otherwise the C code raises an error and
  the barrier is retried, leaving the local state untouched.
  -/
  | absorb (c : Cluster Backend Page) (b : Backend)
      (hemit : BarrierEmitted c)
      (hok : absorbAllowed (c.localState b) c.shared = true) :
      Step c { c with localState := upd c.localState b c.shared }
  /--
  A process writes a page out: `PageSetChecksum()` stamps a checksum exactly
  when `DataChecksumsNeedWrite()` holds for that process' local state.

  This single rule covers both ordinary backends and the datachecksums worker;
  the worker is just a process that happens to rewrite every page.
  -/
  | writePage (c : Cluster Backend Page) (b : Backend) (p : Page) :
      Step c { c with onDisk := upd c.onDisk p (needWrite (c.localState b)) }
  /--
  The coordinator publishes a new state to shared memory: the
  `XLogCtl->data_checksum_version = ...` assignment inside
  `SetDataChecksumsOnInProgress()`, `SetDataChecksumsOn()`,
  `SetDataChecksumsOff()` and `xlog2_redo()`.

  * `hq` — the previous transition finished; established by the
    `WaitForProcSignalBarrier()` at the end of each of those functions, plus
    the fact that only one coordinator runs at a time.
  * `hedge` — the edge is one `AbsorbDataChecksumsBarrier()` will accept
    (see `coordinatorEdges_absorbable`).
  * `hsweep` — moving to `on` requires every page to be checksummed already;
    discharged by `ProcessAllDatabases()` returning true, and checked in C by
    `SetDataChecksumsOn()` refusing any source state but `inprogress-on`.
  -/
  | advanceShared (c : Cluster Backend Page) (t : ChecksumState)
      (hq : Quiescent c) (hemit : BarrierEmitted c)
      (hedge : barrierTable.contains (c.shared, t) = true)
      (hsweep : t = .on → AllChecksummed c) :
      Step c { c with shared := t }
  /--
  The coordinator persists the new state:
  `ControlFile->data_checksum_version = ...; UpdateControlFile();`

  Keeping this separate from `advanceShared` is what exposes the crash window
  that `SetDataChecksumsOn()` reasons about:

  > Update the controlfile before waiting since if we have an immediate
  > shutdown while waiting we want to come back up with checksums enabled.
  -/
  | advanceControl (c : Cluster Backend Page) :
      Step c { c with control := c.shared }
  /--
  The cluster crashes (or is shut down immediately) and restarts.

  Shared memory is lost and rebuilt from `pg_control` by `XLOGShmemInit()`;
  `StartupXLOG()` then applies its end-of-recovery fixups (`recover`) and
  copies the result back into the control file.  Data pages survive unchanged.
  -/
  | restart (c : Cluster Backend Page) :
      Step c { shared := recover c.control,
               control := recover c.control,
               localState := fun _ => recover c.control,
               onDisk := c.onDisk }

/--
A freshly initialised cluster.

`initdb --data-checksums` and an offline `pg_checksums --enable` both leave
every page checksummed and the state at `on`; `initdb --no-data-checksums` and
`pg_checksums --disable` leave it at `off`.  Neither ever produces an
`inprogress-*` state, which is why `pg_checksums` refuses to operate on a data
directory left in one.
-/
def Init {Backend Page : Type} (c : Cluster Backend Page) : Prop :=
  (c.shared = .off ∨ c.shared = .on)
  ∧ c.control = c.shared
  ∧ (∀ b, c.localState b = c.shared)
  ∧ (c.shared = .on → AllChecksummed c)

/-- States the cluster can actually get into. -/
inductive Reachable {Backend Page : Type} [DecidableEq Backend] [DecidableEq Page] :
    Cluster Backend Page → Prop
  | init {c : Cluster Backend Page} (h : Init c) : Reachable c
  | step {c c' : Cluster Backend Page} (h : Reachable c) (hs : Step c c') : Reachable c'

/-- Convenience: take a step and then rewrite the result into a nicer shape. -/
theorem Reachable.step_eq {Backend Page : Type} [DecidableEq Backend] [DecidableEq Page]
    {c c' c'' : Cluster Backend Page}
    (h : Reachable c) (hs : Step c c') (he : c' = c'') : Reachable c'' :=
  he ▸ Reachable.step h hs

end Spec
