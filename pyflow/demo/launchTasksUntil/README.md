# launchTasksUntil Demo

This demo shows how a pyflow script can be configured to launch tasks until a certain amount of work is accomplished,
where the quantity of work accomplished by each task is unknown at the time the tasks are launched. In this case
the total work accomplished by all complete tasks is enumerated, and task launch is canceled once the required total
is reached. Once the total work threshold has been met, any remaining queued or running tasks are canceled.

Note this is an advanced pyflow demo, it should not be used as a tutorial to learn pyflow's basic operation, and it
reflects a use case that was not considered during the original development of pyflow's API. On the latter point,
note in particular that the re-entrant lock used to synchronize the counting of total work accomplished in this demo
must be placed in a special `DeepCopyProtector` object to prevent an attempted deep-copy of the lock. Pyflow workflow
tasks are deep-copied by default, so this workaround is required for any object to be shared between a workflow task
and the code which launches it (except via the filesystem -- which is the communication pathway encouraged by the
pyflow design).
