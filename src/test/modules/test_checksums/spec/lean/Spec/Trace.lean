/-
  A concrete execution, so that the safety theorems are not vacuous.

  A proof that "nothing bad happens" is worthless if nothing happens at all.
  This file exhibits an explicit trace of a two-backend, one-page cluster that
  runs a complete online enable, and in particular passes through the state the
  whole design exists to make safe: one backend already verifying checksums
  while another has not yet absorbed the barrier and is only writing them.
-/
import Spec.Safety

namespace Spec
namespace Trace

open ChecksumState Cluster

/-- Two backends and a single data page. -/
abbrev C := Cluster Bool Unit

/-- Checksums off; the page carries no checksum. -/
def c0 : C := ⟨.off, .off, fun _ => .off, fun _ => false⟩
/-- `SetDataChecksumsOnInProgress()` has published `inprogress-on` to `XLogCtl`. -/
def c1 : C := ⟨.inprogressOn, .off, fun _ => .off, fun _ => false⟩
/-- ... and to `pg_control`, so the barrier goes out. -/
def c2 : C := ⟨.inprogressOn, .inprogressOn, fun _ => .off, fun _ => false⟩
/-- Backend `false` has absorbed it, backend `true` has not. -/
def c3 : C := ⟨.inprogressOn, .inprogressOn, fun b => cond b .off .inprogressOn, fun _ => false⟩
/-- Both backends are at `inprogress-on`; `SetDataChecksumsOnInProgress()` returns. -/
def c4 : C := ⟨.inprogressOn, .inprogressOn, fun _ => .inprogressOn, fun _ => false⟩
/-- The datachecksums worker has rewritten the page with a checksum. -/
def c5 : C := ⟨.inprogressOn, .inprogressOn, fun _ => .inprogressOn, fun _ => true⟩
/-- `SetDataChecksumsOn()` has published `on` to `XLogCtl`. -/
def c6 : C := ⟨.on, .inprogressOn, fun _ => .inprogressOn, fun _ => true⟩
/-- ... and to `pg_control`; this is the window an immediate shutdown can hit. -/
def c7 : C := ⟨.on, .on, fun _ => .inprogressOn, fun _ => true⟩
/-- **The interesting state**: backend `false` verifies, backend `true` still
only writes. -/
def c8 : C := ⟨.on, .on, fun b => cond b .inprogressOn .on, fun _ => true⟩
/-- Everyone is at `on`; `SetDataChecksumsOn()` returns and checksums are
enabled cluster-wide. -/
def c9 : C := ⟨.on, .on, fun _ => .on, fun _ => true⟩

theorem reachable_c0 : Reachable c0 :=
  .init ⟨Or.inl rfl, rfl, fun _ => rfl, fun h => absurd h (by decide)⟩

theorem reachable_c8 : Reachable c8 := by
  have h1 : Reachable c1 :=
    reachable_c0.step_eq
      (Step.advanceShared c0 .inprogressOn (fun _ => rfl) rfl (by decide)
        (fun h => absurd h (by decide))) rfl
  have h2 : Reachable c2 := h1.step_eq (Step.advanceControl c1) rfl
  have h3 : Reachable c3 :=
    h2.step_eq (Step.absorb c2 false rfl (by decide))
      (Cluster.ext rfl rfl (fun b => by cases b <;> rfl) (fun _ => rfl))
  have h4 : Reachable c4 :=
    h3.step_eq (Step.absorb c3 true rfl (by decide))
      (Cluster.ext rfl rfl (fun b => by cases b <;> rfl) (fun _ => rfl))
  have h5 : Reachable c5 :=
    h4.step_eq (Step.writePage c4 false ())
      (Cluster.ext rfl rfl (fun _ => rfl) (fun p => by cases p; rfl))
  have h6 : Reachable c6 :=
    h5.step_eq
      (Step.advanceShared c5 .on (fun _ => rfl) rfl (by decide) (fun _ _ => rfl)) rfl
  have h7 : Reachable c7 := h6.step_eq (Step.advanceControl c6) rfl
  exact h7.step_eq (Step.absorb c7 false rfl (by decide))
    (Cluster.ext rfl rfl (fun b => by cases b <;> rfl) (fun _ => rfl))

theorem reachable_c9 : Reachable c9 :=
  reachable_c8.step_eq (Step.absorb c8 true rfl (by decide))
    (Cluster.ext rfl rfl (fun b => by cases b <;> rfl) (fun _ => rfl))

/-- `c8` really is the mixed state: one backend verifies, the other does not,
and the one that does not still writes. -/
theorem c8_is_mixed :
    needVerify (c8.localState false) = true
    ∧ needVerify (c8.localState true) = false
    ∧ needWrite (c8.localState true) = true := by decide

/-- The cluster does reach full verification. -/
theorem c9_verifies : ∀ b, needVerify (c9.localState b) = true := by decide

/-- Safety applied to a state that genuinely verifies checksums: the page the
worker rewrote is guaranteed to carry one. -/
theorem c8_page_checksummed : ∀ p, c8.onDisk p = true :=
  fun p => reachable_readSafe reachable_c8 false p c8_is_mixed.1

end Trace
end Spec
