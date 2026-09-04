/-
  Data checksum states, and the reader API built on top of them.

  This file mirrors:

    src/include/storage/checksum.h    ChecksumStateType
    src/backend/access/transam/xlog.c DataChecksumsNeedWrite()
                                      DataChecksumsNeedVerify()
                                      DataChecksumsOn() / Off() / InProgressOn()
-/

namespace Spec

/--
The four data checksum states a cluster can be in.

Mirrors `ChecksumStateType` in `src/include/storage/checksum.h`.  The wire
encoding matters, because the value is stored in `pg_control` and shipped in
WAL, so it is spelled out in `toVersion` below rather than being left to the
order of the constructors.
-/
inductive ChecksumState
  /-- `PG_DATA_CHECKSUM_OFF`: checksums are neither written nor verified. -/
  | off
  /-- `PG_DATA_CHECKSUM_VERSION`: checksums are written and verified. -/
  | on
  /-- `PG_DATA_CHECKSUM_INPROGRESS_OFF`: written, but no longer verified. -/
  | inprogressOff
  /-- `PG_DATA_CHECKSUM_INPROGRESS_ON`: written, but not yet verified. -/
  | inprogressOn
  deriving DecidableEq, Repr

namespace ChecksumState

/-- The on-disk / on-the-wire encoding, as in `ChecksumStateType`. -/
def toVersion : ChecksumState → Nat
  | .off           => 0
  | .on            => 1
  | .inprogressOff => 2
  | .inprogressOn  => 3

/-- Every state, used to phrase the finite audits as decidable propositions. -/
def all : List ChecksumState := [.off, .on, .inprogressOff, .inprogressOn]

theorem mem_all (s : ChecksumState) : s ∈ all := by
  cases s <;> decide

/-- The encoding is injective, i.e. the four states really are distinct. -/
theorem toVersion_inj {a b : ChecksumState} (h : a.toVersion = b.toVersion) :
    a = b := by
  cases a <;> cases b <;> simp_all [toVersion]

end ChecksumState

open ChecksumState

/-! ## The reader API

Every consumer of the checksum state in the backend goes through one of the two
predicates below, evaluated against the *backend-local* copy of the state
(`LocalDataChecksumState` in `xlog.c`).  Both are pure functions of that copy,
which is what makes the whole analysis tractable: a backend's behaviour is
determined by its own local state and nothing else.
-/

/--
`DataChecksumsNeedWrite()`.

Called from `PageSetChecksum()` (`bufpage.c`) and, via `XLogHintBitIsNeeded()`,
from the hint-bit paths in `bufmgr.c`, `pruneheap.c`, `visibilitymap.c` and
`freespace.c`.
-/
def needWrite : ChecksumState → Bool
  | .on            => true
  | .inprogressOn  => true
  | .inprogressOff => true
  | .off           => false

/--
`DataChecksumsNeedVerify()`.

Called from `PageIsVerified()` (`bufpage.c`) and from
`backup_checksums_verifiable()` (`basebackup.c`).
-/
def needVerify : ChecksumState → Bool
  | .on => true
  | _   => false

/-- The comment on `DataChecksumsNeedWrite()` says "enabled, or in the process
of being enabled [or disabled]"; that is exactly "not off". -/
theorem needWrite_iff_ne_off (s : ChecksumState) :
    needWrite s = true ↔ s ≠ .off := by
  cases s <;> simp [needWrite]

/-- A backend that verifies checksums also writes them.  This is what makes
`on` a single state rather than two independent flags. -/
theorem needVerify_le_needWrite {s : ChecksumState} (h : needVerify s = true) :
    needWrite s = true := by
  cases s <;> simp_all [needVerify, needWrite]

/-- `needVerify` pins down the state completely. -/
theorem needVerify_eq_on {s : ChecksumState} (h : needVerify s = true) : s = .on := by
  cases s <;> simp_all [needVerify]

/--
Two backends sitting on either side of an in-flight state change coexist
safely when nobody verifies a checksum that somebody else is allowed not to
write.

This is the local, two-backend form of the cluster-wide contract in the
`datachecksum_state.c` header comment:

> all backends MUST calculate and write data checksums during all states
> except off; MUST validate checksums only in the 'on' state.
-/
def mixSafe (a b : ChecksumState) : Bool :=
  (!(needVerify a || needVerify b)) || (needWrite a && needWrite b)

/-- `mixSafe` is symmetric, as a compatibility relation should be. -/
theorem mixSafe_comm (a b : ChecksumState) : mixSafe a b = mixSafe b a := by
  cases a <;> cases b <;> rfl

/-- The only unsafe pairings are `on` with `off`. -/
theorem mixSafe_iff (a b : ChecksumState) :
    mixSafe a b = false ↔ ((a = .on ∧ b = .off) ∨ (a = .off ∧ b = .on)) := by
  cases a <;> cases b <;> simp [mixSafe, needVerify, needWrite]

end Spec
