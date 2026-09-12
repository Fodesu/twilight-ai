package turntest

// The suite is Store-parameterized like agent/session/sessiontest and
// agent/session/run/runtimetest: the Memory store and every durable adapter run
// the same assertions. Assertions on the companion mapping (TRN-CMP, TRN-MAP)
// live in the RUN-CMP-2 suite, which observes the companion through the
// Runtime's group composition; this suite covers the Coordinator, the surface
// projection and the recovery table.
