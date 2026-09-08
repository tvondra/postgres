---------------------------- MODULE DataChecksums ----------------------------
(***************************************************************************)
(* A model of the data checksum state machine, transcribed from the Lean   *)
(* development in the sibling lean/ directory -- specifically              *)
(* lean/Spec/Cluster.lean, whose four state components and six             *)
(* transitions appear here one for one.                                    *)
(*                                                                         *)
(* The Lean development proves safety for an arbitrary number of backends  *)
(* and pages.  This module exists for the things a proof assistant is bad  *)
(* at: liveness, and printing the interleaving that breaks an invariant.   *)
(* See the README in the parent directory.                                 *)
(*                                                                         *)
(* The C code being modelled:                                              *)
(*                                                                         *)
(*   src/include/storage/checksum.h              ChecksumStateType         *)
(*   src/backend/access/transam/xlog.c           the three copies of the   *)
(*                                               state, the readers, and   *)
(*                                               SetDataChecksums*()       *)
(*   src/backend/postmaster/datachecksum_state.c checksum_barriers[] and   *)
(*                                               AbsorbDataChecksumsBarrier*)
(*   src/backend/storage/page/bufpage.c          PageSetChecksum()         *)
(***************************************************************************)
EXTENDS FiniteSets

CONSTANTS
    Backends,   \* processes that can read or write a page
    Pages,      \* data pages in the cluster
    Variant     \* which of the deliberately broken tables to use, if any

Variants == { "none", "brokenTable", "livelock" }

ASSUME Variant \in Variants

(***************************************************************************)
(* The four states, and their on-disk encoding.  Mirrors ChecksumStateType *)
(* in src/include/storage/checksum.h; the encoding matters because the     *)
(* value is stored in pg_control and shipped in WAL.                       *)
(***************************************************************************)
Off           == "off"              \* PG_DATA_CHECKSUM_OFF
On            == "on"               \* PG_DATA_CHECKSUM_VERSION
InProgressOff == "inprogressOff"    \* PG_DATA_CHECKSUM_INPROGRESS_OFF
InProgressOn  == "inprogressOn"     \* PG_DATA_CHECKSUM_INPROGRESS_ON

ChecksumState == { Off, On, InProgressOff, InProgressOn }

Version(s) == CASE s = Off           -> 0
                [] s = On            -> 1
                [] s = InProgressOff -> 2
                [] s = InProgressOn  -> 3

(***************************************************************************)
(* The reader API.  Every consumer of the checksum state in the backend    *)
(* goes through one of these two predicates, evaluated against the         *)
(* process' *own* copy of the state, so a process' behaviour is determined *)
(* by local[b] and nothing else.                                           *)
(*                                                                         *)
(*   NeedWrite   DataChecksumsNeedWrite(), called from PageSetChecksum()   *)
(*               and, via XLogHintBitIsNeeded(), from the hint-bit paths   *)
(*               in bufmgr.c, pruneheap.c, visibilitymap.c and freespace.c *)
(*   NeedVerify  DataChecksumsNeedVerify(), called from PageIsVerified()   *)
(*               and from backup_checksums_verifiable()                    *)
(***************************************************************************)
NeedWrite(s)  == s # Off
NeedVerify(s) == s = On

(***************************************************************************)
(* checksum_barriers[9] from datachecksum_state.c, with the C comments     *)
(* kept so the two can be diffed by eye.                                   *)
(***************************************************************************)
BaseBarrierTable ==
    {   \* Disabling checksums: If checksums are currently enabled, disabling
        \* must go through the 'inprogress-off' state.
        <<On,            InProgressOff>>,
        <<InProgressOff, Off>>,
        \* If checksums are in the process of being enabled, but are not yet
        \* being verified, we can abort by going back to 'off' state.
        <<InProgressOn,  Off>>,
        \* Enabling checksums must normally go through the 'inprogress-on'
        \* state.
        <<Off,           InProgressOn>>,
        <<InProgressOn,  On>>,
        \* If checksums are being disabled but all backends are still computing
        \* checksums, we can go straight back to 'on'
        <<InProgressOff, On>>,
        \* If checksums are being enabled when launcher_exit is executed, state
        \* is set to off since we cannot reach on at that point.
        <<InProgressOn,  InProgressOff>>,
        \* Transitions that can happen when a new request is made while another
        \* is currently being processed.
        <<InProgressOff, InProgressOn>>,
        <<Off,           InProgressOff>>    }

(***************************************************************************)
(* The two deliberate defects, selected by the Variant constant so that    *)
(* each .cfg file in this directory is a single scalar apart from the      *)
(* others.  Both add the same shortcut edge, but in different places, and  *)
(* they fail in interestingly different ways:                              *)
(*                                                                         *)
(*   brokenTable  the edge is added to checksum_barriers[], so a backend   *)
(*                will happily absorb it.  A safety violation; this is     *)
(*                lean/Spec/Safety.lean's onOff_shortcut_unsafe made       *)
(*                executable.                                              *)
(*   livelock     the edge is only driven by the coordinator, so no        *)
(*                running backend can absorb it and the handshake never    *)
(*                retires.  A liveness violation, which the Lean           *)
(*                development cannot express at all -- its advanceShared   *)
(*                rule requires a table edge by construction.              *)
(*                                                                         *)
(*                It is worse than a livelock, though.  BackendStart is    *)
(*                not gated on the table, so a *newly forked* backend      *)
(*                adopts the illegal target directly and the same variant  *)
(*                violates WriteSafe as well; see the README.              *)
(***************************************************************************)
ExtraBarrierEdges     == IF Variant = "brokenTable" THEN { <<On, Off>> } ELSE {}
ExtraCoordinatorEdges == IF Variant = "livelock"    THEN { <<On, Off>> } ELSE {}

BarrierTable == BaseBarrierTable \union ExtraBarrierEdges

(***************************************************************************)
(* AbsorbDataChecksumsBarrier().  The first disjunct is the early return   *)
(*                                                                         *)
(*     if (current == target_state)                                        *)
(*         return true;                                                    *)
(*                                                                         *)
(* which makes a repeated barrier a no-op; the second is the table lookup  *)
(* that otherwise ereport(ERROR)s.                                         *)
(***************************************************************************)
AbsorbAllowed(a, b) == a = b \/ <<a, b>> \in BarrierTable

(***************************************************************************)
(* The transitions the coordinator drives.  Keeping this *separate* from   *)
(* BarrierTable is the one place where this module is deliberately more    *)
(* liberal than lean/Spec/Cluster.lean, whose advanceShared rule requires  *)
(* the edge to be in the barrier table by construction.  The C code has no *)
(* such guarantee -- SetDataChecksums*() and xlog2_redo() assign to        *)
(* XLogCtl->data_checksum_version directly -- so the agreement between the *)
(* two tables is a property to be checked, not an assumption.  That is     *)
(* what Lean's coordinatorEdges_absorbable theorem states, and what        *)
(* Livelock.cfg violates on purpose.                                       *)
(***************************************************************************)
CoordinatorEdges == BarrierTable \union ExtraCoordinatorEdges

(***************************************************************************)
(* The end-of-recovery fixups in StartupXLOG(): an interrupted transition  *)
(* is resolved to 'off', because the sweep cannot be resumed and no        *)
(* backend can still be verifying.                                         *)
(***************************************************************************)
Recover(s) == IF s \in { InProgressOn, InProgressOff } THEN Off ELSE s

-----------------------------------------------------------------------------

VARIABLES
    shared,     \* XLogCtl->data_checksum_version, under info_lck
    control,    \* ControlFile->data_checksum_version, the durable copy
    local,      \* LocalDataChecksumState, one per process
    onDisk      \* whether the page on disk carries a valid checksum

vars == <<shared, control, local, onDisk>>

TypeOK ==
    /\ shared  \in ChecksumState
    /\ control \in ChecksumState
    /\ local   \in [Backends -> ChecksumState]
    /\ onDisk  \in [Pages -> BOOLEAN]

(***************************************************************************)
(* Cluster.Quiescent: every process has caught up, i.e. the last           *)
(* WaitForProcSignalBarrier() has returned.                                *)
(***************************************************************************)
Quiescent == \A b \in Backends : local[b] = shared

(***************************************************************************)
(* Cluster.BarrierEmitted: every writer updates the control file before    *)
(* calling EmitProcSignalBarrier(), so a barrier is in flight only once    *)
(* control has caught up with shared.                                      *)
(***************************************************************************)
BarrierEmitted == control = shared

AllChecksummed == \A p \in Pages : onDisk[p]

(***************************************************************************)
(* Cluster.Init: initdb and offline pg_checksums leave the cluster in one  *)
(* of the two stable states, never in an 'inprogress-*' one.               *)
(***************************************************************************)
Init ==
    /\ shared \in { Off, On }
    /\ control = shared
    /\ local = [b \in Backends |-> shared]
    /\ onDisk \in [Pages -> BOOLEAN]
    /\ (shared = On) => AllChecksummed

-----------------------------------------------------------------------------
(***************************************************************************)
(* The six transitions of lean/Spec/Cluster.lean's Step relation.          *)
(*                                                                         *)
(* Reads are not transitions: DataChecksumsNeedVerify() and friends change *)
(* nothing, so "a read misbehaves" is a property of a state rather than of *)
(* a step.  That is why the invariants below say everything there is to    *)
(* say about the readers.                                                  *)
(***************************************************************************)

(***************************************************************************)
(* Step.backendStart -- InitLocalDataChecksumState(), called from          *)
(* InitPostgres() and AuxiliaryProcessMainCommon().  Note it reads shared, *)
(* not control, and is not gated on a barrier having been emitted.         *)
(***************************************************************************)
BackendStart(b) ==
    /\ local[b] # shared
    /\ local' = [local EXCEPT ![b] = shared]
    /\ UNCHANGED <<shared, control, onDisk>>

(***************************************************************************)
(* Step.absorb -- AbsorbDataChecksumsBarrier().  The target is shared,     *)
(* because every writer publishes to XLogCtl before emitting the barrier   *)
(* that announces the new value.                                           *)
(*                                                                         *)
(* When the edge is rejected the C code raises an error and the procsignal *)
(* machinery retries, leaving the local state untouched; that is modelled  *)
(* here by the action simply not being enabled, which is what turns the    *)
(* retry loop into the stuttering lasso Livelock.cfg finds.                *)
(***************************************************************************)
Absorb(b) ==
    /\ BarrierEmitted
    /\ local[b] # shared
    /\ AbsorbAllowed(local[b], shared)
    /\ local' = [local EXCEPT ![b] = shared]
    /\ UNCHANGED <<shared, control, onDisk>>

(***************************************************************************)
(* Step.writePage -- PageSetChecksum() stamps a checksum exactly when      *)
(* DataChecksumsNeedWrite() holds for the writing process.  This one rule  *)
(* covers ordinary backends and the datachecksums worker alike; the worker *)
(* is just a process that happens to rewrite every page.                   *)
(***************************************************************************)
WritePage(b, p) ==
    /\ onDisk' = [onDisk EXCEPT ![p] = NeedWrite(local[b])]
    /\ UNCHANGED <<shared, control, local>>

(***************************************************************************)
(* Step.advanceShared -- the XLogCtl->data_checksum_version assignment     *)
(* inside SetDataChecksumsOnInProgress(), SetDataChecksumsOn(),            *)
(* SetDataChecksumsOff() and xlog2_redo().                                 *)
(*                                                                         *)
(* Quiescent holds because of the WaitForProcSignalBarrier() ending each   *)
(* of those functions, plus the fact that only one coordinator runs at a   *)
(* time: the launcher is a singleton on a primary, and the startup process *)
(* is the only replayer on a standby.                                      *)
(*                                                                         *)
(* Moving to On additionally requires every page to be checksummed, which  *)
(* is what ProcessAllDatabases() establishes and what SetDataChecksumsOn() *)
(* enforces by refusing any source state but 'inprogress-on'.              *)
(***************************************************************************)
AdvanceShared(t) ==
    /\ Quiescent
    /\ BarrierEmitted
    /\ <<shared, t>> \in CoordinatorEdges
    /\ (t = On) => AllChecksummed
    /\ shared' = t
    /\ UNCHANGED <<control, local, onDisk>>

(***************************************************************************)
(* Step.advanceControl -- ControlFile->data_checksum_version plus          *)
(* UpdateControlFile(), under ControlFileLock.                             *)
(*                                                                         *)
(* Keeping this separate from AdvanceShared is what exposes the crash      *)
(* window SetDataChecksumsOn() reasons about:                              *)
(*                                                                         *)
(*   Update the controlfile before waiting since if we have an immediate   *)
(*   shutdown while waiting we want to come back up with checksums enabled.*)
(***************************************************************************)
AdvanceControl ==
    /\ control # shared
    /\ control' = shared
    /\ UNCHANGED <<shared, local, onDisk>>

(***************************************************************************)
(* Step.restart -- a crash or immediate shutdown.  Shared memory is lost   *)
(* and rebuilt from pg_control by XLOGShmemInit(); StartupXLOG() applies   *)
(* its fixups and copies the result back.  Data pages survive unchanged.   *)
(***************************************************************************)
Restart ==
    /\ shared'  = Recover(control)
    /\ control' = Recover(control)
    /\ local'   = [b \in Backends |-> Recover(control)]
    /\ UNCHANGED onDisk

Next ==
    \/ \E b \in Backends            : BackendStart(b)
    \/ \E b \in Backends            : Absorb(b)
    \/ \E b \in Backends, p \in Pages : WritePage(b, p)
    \/ \E t \in ChecksumState       : AdvanceShared(t)
    \/ AdvanceControl
    \/ Restart

(***************************************************************************)
(* Fairness.  Only the two actions that make up the barrier handshake are  *)
(* fair:                                                                   *)
(*                                                                         *)
(*   AdvanceControl  the coordinator does get as far as UpdateControlFile()*)
(*                   and EmitProcSignalBarrier()                           *)
(*   Absorb(b)       a backend that has a barrier pending does eventually  *)
(*                   run ProcessProcSignalBarrier()                        *)
(*                                                                         *)
(* BackendStart is deliberately *not* fair.  A running backend cannot call *)
(* InitLocalDataChecksumState(); only a newly forked one does, and no      *)
(* progress argument may lean on new connections arriving.                 *)
(*                                                                         *)
(* AdvanceShared is not fair either, so nothing here claims that a         *)
(* requested enable finishes -- that would need the launcher's goal to be  *)
(* modelled.  Restart is not fair for the obvious reason.                  *)
(***************************************************************************)
Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(AdvanceControl)
    /\ \A b \in Backends : WF_vars(Absorb(b))

-----------------------------------------------------------------------------
(***************************************************************************)
(* Safety.  These are lean/Spec/Safety.lean's reachable_writeSafe,         *)
(* reachable_readSafe and no_spurious_failure, restricted to the finite    *)
(* Backends and Pages given in the .cfg file.                              *)
(***************************************************************************)

(***************************************************************************)
(* If any process verifies data checksums, every process writes them.      *)
(* datachecksum_state.c:                                                   *)
(*                                                                         *)
(*   all backends MUST calculate and write data checksums during all       *)
(*   states except off; MUST validate checksums only in the 'on' state.    *)
(***************************************************************************)
WriteSafe ==
    \A b1, b2 \in Backends : NeedVerify(local[b1]) => NeedWrite(local[b2])

(***************************************************************************)
(* If any process verifies data checksums, every page on disk carries one. *)
(***************************************************************************)
ReadSafe ==
    \A b \in Backends : NeedVerify(local[b]) => AllChecksummed

(***************************************************************************)
(* The corollary the feature is really about: online enabling and          *)
(* disabling never makes PageIsVerified() report a checksum failure on a   *)
(* page the cluster itself wrote.                                          *)
(***************************************************************************)
NoSpuriousFailure ==
    \A b \in Backends, p \in Pages : ~(NeedVerify(local[b]) /\ ~onDisk[p])

-----------------------------------------------------------------------------
(***************************************************************************)
(* The barrier table audit.  These are constant-level facts -- they do not *)
(* mention the variables -- and correspond to the theorems proved by       *)
(* `decide` in lean/Spec/Safety.lean.  They are listed as invariants so    *)
(* that TLC evaluates them; anyone editing checksum_barriers[] should      *)
(* look here first.                                                        *)
(***************************************************************************)

(***************************************************************************)
(* Two processes on either side of an in-flight change coexist safely when *)
(* nobody verifies a checksum that somebody else need not write.           *)
(***************************************************************************)
MixSafe(a, b) ==
    (NeedVerify(a) \/ NeedVerify(b)) => (NeedWrite(a) /\ NeedWrite(b))

TableAudit ==
    \* barrierTable_length
    /\ Cardinality(BaseBarrierTable) = 9
    \* barrierTable_mixSafe
    /\ \A e \in BarrierTable : MixSafe(e[1], e[2])
    \* barrierTable_no_direct_toggle: the entire reason the two
    \* 'inprogress-*' states exist
    /\ <<On,  Off>> \notin BarrierTable
    /\ <<Off, On >> \notin BarrierTable
    \* barrierTable_pairwiseSafe.  Read it as: while a barrier for g is in
    \* flight, the processes are split between g and the single state they
    \* all shared beforehand.  Whichever two you pick, if one verifies then
    \* the other writes.
    /\ \A a, b, g \in ChecksumState :
          ( /\ AbsorbAllowed(a, g)
            /\ AbsorbAllowed(b, g)
            /\ (a = g \/ b = g)
            /\ NeedVerify(a) ) => NeedWrite(b)
    \* coordinatorEdges_absorbable.  If this fails, a backend absorbing the
    \* barrier takes the ereport(ERROR, ... "incorrect data checksum state
    \* %d for target state %d") path, procsignal resets the bit and retries
    \* forever, and WaitForProcSignalBarrier() in the coordinator never
    \* returns.
    /\ \A e \in CoordinatorEdges : AbsorbAllowed(e[1], e[2])

-----------------------------------------------------------------------------
(***************************************************************************)
(* Liveness -- the reason this module exists alongside the Lean proof.     *)
(*                                                                         *)
(* Whenever some process is behind, the handshake retires: every pending   *)
(* barrier is eventually absorbed and WaitForProcSignalBarrier() returns.  *)
(* This is a temporal property, so Lean can only approach it indirectly,   *)
(* by proving that the coordinator never emits a rejected edge.  Here it   *)
(* is checked directly, and Livelock.cfg shows what its failure looks      *)
(* like.                                                                   *)
(***************************************************************************)
BarrierRetires == ~Quiescent ~> Quiescent

-----------------------------------------------------------------------------
(***************************************************************************)
(* The inductive invariant of lean/Spec/Safety.lean, field for field.      *)
(* Checking it with INIT Inv / NEXT Next -- rather than from the real      *)
(* Init -- is an independent test of the Lean Inv.step proof: TLC tries    *)
(* every state satisfying Inv, not just the reachable ones.                *)
(***************************************************************************)
Inv ==
    /\ TypeOK
    \* localCompat: every process is at shared or one legal edge behind it
    /\ \A b \in Backends : AbsorbAllowed(local[b], shared)
    \* atMostTwo: of any two processes, one has caught up, or they agree.
    \* This is what WaitForProcSignalBarrier() plus a single coordinator
    \* buys, and it is what the naive "everyone is barrier-compatible with
    \* shared" invariant is missing: without it, one process at On and
    \* another at Off would be admissible.
    /\ \A b1, b2 \in Backends :
          \/ local[b1] = shared
          \/ local[b2] = shared
          \/ local[b1] = local[b2]
    \* sharedOnWrites / sharedOnPages
    /\ (shared = On) => \A b \in Backends : NeedWrite(local[b])
    /\ (shared = On) => AllChecksummed
    \* controlOnShared / controlOnWrites / controlOnPages: what bounds the
    \* window opened by updating the control file before waiting, and what
    \* makes coming back up in On after an immediate shutdown safe
    /\ (control = On) => NeedWrite(shared)
    /\ (control = On) => \A b \in Backends : NeedWrite(local[b])
    /\ (control = On) => AllChecksummed
    \* readSafe, carried inductively
    /\ ReadSafe

=============================================================================
