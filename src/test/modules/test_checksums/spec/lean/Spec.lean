/-
  A formal model of the data checksum state machine.

  Root module: importing `Spec` pulls in the whole development.  See the README
  in this directory for the correspondence with the C code and for the
  modelling assumptions.
-/
import Spec.State
import Spec.Barriers
import Spec.Cluster
import Spec.Safety
import Spec.Trace
