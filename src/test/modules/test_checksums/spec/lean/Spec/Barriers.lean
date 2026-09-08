/-
  The procsignal barrier table and the transitions the coordinator performs.

  This file mirrors:

    src/backend/postmaster/datachecksum_state.c  checksum_barriers[9]
                                                 AbsorbDataChecksumsBarrier()
                                                 EmitAndWaitDataChecksumsBarrier()
    src/backend/access/transam/xlog.c            SetDataChecksumsOnInProgress()
                                                 SetDataChecksumsOn()
                                                 SetDataChecksumsOff()
                                                 StartupXLOG() end-of-recovery
-/
import Spec.State

namespace Spec

open ChecksumState

/-! ## The barrier table

`AbsorbDataChecksumsBarrier()` validates every local state change against
`checksum_barriers`, a nine-entry table of `{from, to}` pairs.  The entries
below are in the same order as in the C array, with the C comments kept so the
two can be diffed by eye.
-/

/-- `checksum_barriers[9]` from `datachecksum_state.c`. -/
def barrierTable : List (ChecksumState × ChecksumState) :=
  [ -- Disabling checksums: If checksums are currently enabled, disabling must
    -- go through the 'inprogress-off' state.
    (.on, .inprogressOff),
    (.inprogressOff, .off),
    -- If checksums are in the process of being enabled, but are not yet being
    -- verified, we can abort by going back to 'off' state.
    (.inprogressOn, .off),
    -- Enabling checksums must normally go through the 'inprogress-on' state.
    (.off, .inprogressOn),
    (.inprogressOn, .on),
    -- If checksums are being disabled but all backends are still computing
    -- checksums, we can go straight back to 'on'
    (.inprogressOff, .on),
    -- If checksums are being enabled when launcher_exit is executed, state is
    -- set to off since we cannot reach on at that point.
    (.inprogressOn, .inprogressOff),
    -- Transitions that can happen when a new request is made while another is
    -- currently being processed.
    (.inprogressOff, .inprogressOn),
    (.off, .inprogressOff) ]

/-- The table has exactly the nine entries the C array is sized for. -/
theorem barrierTable_length : barrierTable.length = 9 := by decide

/--
`AbsorbDataChecksumsBarrier()`, parameterised by the table so that variant
tables can be evaluated too (see `Spec.Safety`).

The `a == b` disjunct is the early return

    if (current == target_state)
        return true;

which treats a repeated barrier as a no-op.
-/
def absorbAllowedIn (tbl : List (ChecksumState × ChecksumState))
    (a b : ChecksumState) : Bool :=
  a == b || tbl.contains (a, b)

/-- `AbsorbDataChecksumsBarrier()` against the real `checksum_barriers`. -/
def absorbAllowed (a b : ChecksumState) : Bool := absorbAllowedIn barrierTable a b

theorem absorbAllowed_refl (a : ChecksumState) : absorbAllowed a a = true := by
  cases a <;> rfl

/-! ## Coordinator transitions

The state changes are all driven from a single coordinator: the datachecksums
launcher on a primary (`DataChecksumsWorkerLauncherMain`), or the startup
process on a standby (`xlog2_redo`, which simply replays the primary's
sequence).  These are the edges it can take.
-/

/-- Every `(from, to)` pair a coordinator can drive the cluster along.

* `off`/`inprogressOff` → `inprogressOn` — `SetDataChecksumsOnInProgress()`
* `inprogressOn` → `on` — `SetDataChecksumsOn()`
* `on`/`inprogressOn` → `inprogressOff` — `SetDataChecksumsOff()`, first phase
* `inprogressOff` → `off` — `SetDataChecksumsOff()`, second phase
* `inprogressOn`/`inprogressOff` → `off` — `StartupXLOG()` end-of-recovery
-/
def coordinatorEdges : List (ChecksumState × ChecksumState) :=
  [ (.off, .inprogressOn),
    (.inprogressOff, .inprogressOn),
    (.inprogressOn, .on),
    (.on, .inprogressOff),
    (.inprogressOn, .inprogressOff),
    (.inprogressOff, .off),
    (.inprogressOn, .off) ]

/--
Every edge the coordinator drives is accepted by `AbsorbDataChecksumsBarrier()`.

If this failed, a backend absorbing the barrier would take the
`ereport(ERROR, ... "incorrect data checksum state %d for target state %d")`
path.  The procsignal machinery would then reset the bit and retry forever,
and `WaitForProcSignalBarrier()` in the coordinator would never return: a
livelock, not a crash, which is exactly the kind of bug that is easy to miss
in testing.
-/
theorem coordinatorEdges_absorbable :
    coordinatorEdges.all (fun e => absorbAllowed e.1 e.2) = true := by decide

/--
The two states the cluster can be left in by a crash, once
`StartupXLOG()` has applied its end-of-recovery fixups:

    if (XLogCtl->data_checksum_version == PG_DATA_CHECKSUM_INPROGRESS_ON)   -> off
    else if (XLogCtl->data_checksum_version == PG_DATA_CHECKSUM_INPROGRESS_OFF) -> off

`inprogress-on` is reverted because the sweep cannot be resumed; `inprogress-off`
is reverted because no backend can still be verifying.
-/
def recover : ChecksumState → ChecksumState
  | .inprogressOn  => .off
  | .inprogressOff => .off
  | s              => s

/-- Recovery only ever lands in one of the two stable states. -/
theorem recover_stable (s : ChecksumState) :
    recover s = .off ∨ recover s = .on := by
  cases s <;> simp [recover]

/-- Recovery never invents verification: coming up in `on` means the control
file already said `on`. -/
theorem recover_eq_on {s : ChecksumState} (h : recover s = .on) : s = .on := by
  cases s <;> simp_all [recover]

/-- The fixups are themselves legal barrier edges, so the barrier
`StartupXLOG()` emits through `EmitAndWaitDataChecksumsBarrier()` can be
absorbed by any backend that is already connected (on a promoting standby). -/
theorem recover_absorbable (s : ChecksumState) : absorbAllowed s (recover s) = true := by
  cases s <;> rfl

end Spec
