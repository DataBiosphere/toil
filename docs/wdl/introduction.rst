.. _wdl:

WDL in Toil
***********

The `Workflow Description Language`_ (WDL) is a programming language designed
for writing workflows that execute a set of tasks in a pipeline distributed
across multiple computers. Workflows enable scientific analyses to be
reproducible, by wrapping up a whole sequence of commands, whose outputs feed
into other commands, into a workflow that can be executed the same way every
time.

Toil can be used to :ref:`run <runWdl>` and to :ref:`develop <devWdl>` WDL
workflows. The Toil team also maintains a set of
:ref:`WDL conformance tests <conformanceWdl>` for evaluating Toil and other WDL
runners.

.. _`Workflow Description Language`: https://openwdl.org/
