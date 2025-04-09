.. _tutorialWdl:

WDL Tutorial
============

This tutorial will walk you through writing a workflow in WDL that you can `run
with Toil <runWdl>`__. We're going to write a workflow for `Fizz
Buzz <https://en.wikipedia.org/wiki/Fizz_buzz>`__.


Writing the File
----------------

Let's go step by step through the process of creating a single-file WDL workflow.

Version
~~~~~~~

All WDL files need to start with a ``version`` statement (unless they
are very old ``draft-2`` files). Toil supports ``draft-2``, WDL 1.0, and
WDL 1.1, while Cromwell (another popular WDL runner used on Terra)
supports only ``draft-2`` and 1.0.

So let's start a new WDL 1.0 workflow. Open up a file named
``fizzbuzz.wdl`` and start with a version statement:

.. code-block::

    version 1.0


Workflow Block
~~~~~~~~~~~~~~

Then, add an empty ``workflow`` named ``FizzBuzz``.

.. code-block::

    version 1.0
    workflow FizzBuzz {
    }



Input Block
~~~~~~~~~~~

Workflows usually need some kind of user input, so let's give our
workflow an ``input`` section.

.. code-block::

    version 1.0
    workflow FizzBuzz {
        input {
            # How many FizzBuzz numbers do we want to make?
            Int item_count
            # Every multiple of this number, we produce "Fizz"
            Int to_fizz = 3
            # Every multiple of this number, we produce "Buzz"
            Int to_buzz = 5
            # Optional replacement for the string to print when a multiple of both
            String? fizzbuzz_override
        }
    }

Notice that each input has a type, a name, and an optional default
value. If the type ends in ``?``, the value is optional, and it may be
``null``. If an input is *not* optional, and there is no default value,
then the user's inputs file *must* specify a value for it in order for
the workflow to run.

Body
~~~~

Now we'll start on the body of the workflow, to be inserted just after
the inputs section.

The first thing we're going to need to do is create an array of all the
numbers up to the ``item_count``. We can do this by calling the WDL
``range()`` function, and assigning the result to an ``Array[Int]``
variable.

.. code-block::

    Array[Int] numbers = range(item_count)

WDL 1.0 has `a wide variety of functions in its standard
library <https://github.com/openwdl/wdl/blob/main/versions/1.0/SPEC.md#standard-library>`__,
and WDL 1.1 has even more.

Scattering
~~~~~~~~~~

Once we create an array of all the numbers, we can use a ``scatter`` to
operate on each. WDL does not have loops; instead it has scatters, which
work a bit like a ``map()`` in Python. The body of the scatter runs for
each value in the input array, all in parallel. We're going to increment
all the numbers, since FizzBuzz starts at 1 but WDL ``range()`` starts
at 0.

.. code-block::

    Array[Int] numbers = range(item_count)
    scatter (i in numbers) {
        Int one_based = i + 1
    }

Conditionals
~~~~~~~~~~~~

Inside the body of the scatter, we are going to put some conditionals to
determine if we should produce ``"Fizz"``, ``"Buzz"``, or
``"FizzBuzz"``. To support our ``fizzbuzz_override``, we use an array of
it and a default value, and use the WDL ``select_first()`` function to
find the first non-null value in that array.

Each execution of a scatter is allowed to declare variables, and outside
the scatter those variables are combined into arrays of all the results.
But each variable can be declared only *once* in the scatter, even with
conditionals. So we're going to use ``select_first()`` at the end and
take advantage of variables from un-executed conditionals being
``null``.

Note that WDL supports conditional *expressions* with a ``then`` and an
``else``, but conditional *statements* only have a body, not an ``else``
branch. If you need an else you will have to check the negated
condition.

So first, let's handle the special cases.

.. code-block::

    Array[Int] numbers = range(item_count)
    scatter (i in numbers) {
       Int one_based = i + 1
       if (one_based % to_fizz == 0) {
           String fizz = "Fizz"
           if (one_based % to_buzz == 0) {
               String fizzbuzz = select_first([fizzbuzz_override, "FizzBuzz"])
           }
       }
       if (one_based % to_buzz == 0) {
           String buzz = "Buzz"
       }
       if (one_based % to_fizz != 0 && one_based % to_buzz != 0) {
           # Just a normal number.
       }
    }



Calling Tasks
~~~~~~~~~~~~~

Now for the normal numbers, we need to convert our number into a string.
In WDL 1.1, and in WDL 1.0 on Cromwell, you can use a ``${}``
substitution syntax in quoted strings anywhere, not just in command line
commands. Toil technically will support this too, but it's not in the
spec, and the tutorial needs an excuse for you to call a task. So we're
going to insert a call to a ``stringify_number`` task, to be written
later.

To call a task (or another workflow), we use a ``call`` statement and
give it some inputs. Then we can fish the output values out of the task
with . access, only if we don't make a noise instead.

.. code-block::

    Array[Int] numbers = range(item_count)
    scatter (i in numbers) {
       Int one_based = i + 1
       if (one_based % to_fizz == 0) {
           String fizz = "Fizz"
           if (one_based % to_buzz == 0) {
               String fizzbuzz = select_first([fizzbuzz_override, "FizzBuzz"])
           }
       }
       if (one_based % to_buzz == 0) {
           String buzz = "Buzz"
       }
       if (one_based % to_fizz != 0 && one_based % to_buzz != 0) {
           # Just a normal number.
           call stringify_number {
               input:
                   the_number = one_based
           }
       }
       String result = select_first([fizzbuzz, fizz, buzz, stringify_number.the_string])
    }

We can put the code into the workflow now, and set about writing the
task.

.. code-block::

    version 1.0
    workflow FizzBuzz {
        input {
            # How many FizzBuzz numbers do we want to make?
            Int item_count
            # Every multiple of this number, we produce "Fizz"
            Int to_fizz = 3
            # Every multiple of this number, we produce "Buzz"
            Int to_buzz = 5
            # Optional replacement for the string to print when a multiple of both
            String? fizzbuzz_override
        }
        Array[Int] numbers = range(item_count)
        scatter (i in numbers) {
            Int one_based = i + 1
            
            if (one_based % to_fizz == 0) {
                String fizz = "Fizz"
                if (one_based % to_buzz == 0) {
                    String fizzbuzz = select_first([fizzbuzz_override, "FizzBuzz"])
                }
             }
            if (one_based % to_buzz == 0) {
                String buzz = "Buzz"
            }
            if (one_based % to_fizz != 0 && one_based % to_buzz != 0) {
                # Just a normal number.
                call stringify_number {
                    input:
                        the_number = one_based
                }
            }
            String result = select_first([fizzbuzz, fizz, buzz, stringify_number.the_string]
        }
    }

Writing Tasks
~~~~~~~~~~~~~

Our task should go after the workflow in the file. It looks a lot like a
workflow except it uses ``task``.

.. code-block::

    task stringify_number {
    }

We're going to want it to take in an integer ``the_number``, and we're
going to want it to output a string ``the_string``. So let's fill that
in in ``input`` and ``output`` sections.

.. code-block::

    task stringify_number {
        input {
            Int the_number
        }
        # ???
        output {
            String the_string # = ???
        }
    }

Now, unlike workflows, tasks can have a ``command`` section, which gives
a command to run. This section is now usually set off with triple angle
brackets, and inside it you can use ``~{}``, that is, Bash-like
substitution but with a tilde, to place WDL variables into your command
script. So let's add a command that will echo back the number so we can
see it as a string.

.. code-block::

    task stringify_number {
        input {
            Int the_number
        }
        command <<<
           # This is a Bash script.
           # So we should do good Bash script things like stop on errors
           set -e
           # Now print our number as a string
           echo ~{the_number}
        >>>
        output {
            String the_string # = ???
        }
    }

Now we need to capture the result of the command script. The WDL
``stdout()`` returns a WDL ``File`` containing the standard output
printed by the task's command. We want to read that back into a string,
which we can do with the WDL ``read_string()`` function (which also
`removes trailing
newlines <https://github.com/openwdl/wdl/blob/main/versions/1.0/SPEC.md#string-read_stringstringfile>`__).

.. code-block::

    task stringify_number {
        input {
            Int the_number
        }
        command <<<
           # This is a Bash script.
           # So we should do good Bash script things like stop on errors
           set -e
           # Now print our number as a string
           echo ~{the_number}
        >>>
        output {
            String the_string = read_string(stdout())
        }
    }

We're also going to want to add a ``runtime`` section to our task, to
specify resource requirements. We're also going to tell it to run in a
Docker container, to make sure that absolutely nothing can go wrong with
our delicate ``echo`` command. In a real workflow, you probably want to
set up optiopnal inputs for all the tasks to let you control the
resource requirements, but here we will just hardcode them.

.. code-block::

    task stringify_number {
        input {
            Int the_number
        }
        command <<<
            # This is a Bash script.
            # So we should do good Bash script things like stop on errors
            set -e
            # Now print our number as a string
            echo ~{the_number}
        >>>
        output {
            String the_string = read_string(stdout())
        }
        runtime {
            cpu: 1
            memory: "0.5 GB"
            disks: "local-disk 1 SSD"
            docker: "ubuntu:24.04"
        }
    }

The ``disks`` section is a little weird; it isn't in the WDL spec, but
Toil supports Cromwell-style strings that ask for a ``local-disk`` of a
certain number of gigabytes, which may suggest that it be ``SSD``
storage.

Then we can put our task into our WDL file:

.. code-block::

    version 1.0
    workflow FizzBuzz {
        input {
            # How many FizzBuzz numbers do we want to make?
            Int item_count
            # Every multiple of this number, we produce "Fizz"
            Int to_fizz = 3
            # Every multiple of this number, we produce "Buzz"
            Int to_buzz = 5
            # Optional replacement for the string to print when a multiple of both
            String? fizzbuzz_override
        }
        Array[Int] numbers = range(item_count)
        scatter (i in numbers) {
            Int one_based = i + 1
            if (one_based % to_fizz == 0) {
                String fizz = "Fizz"
                if (one_based % to_buzz == 0) {
                    String fizzbuzz = select_first([fizzbuzz_override, "FizzBuzz"])
                }
             }
            if (one_based % to_buzz == 0) {
                String buzz = "Buzz"
            }
            if (one_based % to_fizz != 0 && one_based % to_buzz != 0) {
                # Just a normal number.
                call stringify_number {
                    input:
                        the_number = one_based
                }
            }
            String result = select_first([fizzbuzz, fizz, buzz, stringify_number.the_string]
        }
    }
    task stringify_number {
        input {
            Int the_number
        }
        command <<<
            # This is a Bash script.
            # So we should do good Bash script things like stop on errors
            set -e
            # Now print our number as a string
            echo ~{the_number}
        >>>
        output {
            String the_string = read_string(stdout())
        }
        runtime {
            cpu: 1
            memory: "0.5 GB"
            disks: "local-disk 1 SSD"
            docker: "ubuntu:24.04"
        }
    }



Output Block
~~~~~~~~~~~~

Now the only thing missing is a workflow-level ``output`` section.
Technically, in WDL 1.0 you aren't supposed to need this, but you do
need it in 1.1 and Toil doesn't actually send your outputs anywhere yet
if you don't have one, so we're going to make one. We need to collect
together all the strings that came out of the different tasks in our
scatter into an ``Array[String]``. We'll add the ``output`` section at
the end of the ``workflow`` section, above the task.

.. code-block::

    version 1.0
    workflow FizzBuzz {
        input {
            # How many FizzBuzz numbers do we want to make?
            Int item_count
            # Every multiple of this number, we produce "Fizz"
            Int to_fizz = 3
            # Every multiple of this number, we produce "Buzz"
            Int to_buzz = 5
            # Optional replacement for the string to print when a multiple of both
            String? fizzbuzz_override
        }
        Array[Int] numbers = range(item_count)
        scatter (i in numbers) {
            Int one_based = i + 1
            if (one_based % to_fizz == 0) {
                String fizz = "Fizz"
                if (one_based % to_buzz == 0) {
                    String fizzbuzz = select_first([fizzbuzz_override, "FizzBuzz"])
                }
             }
            if (one_based % to_buzz == 0) {
                String buzz = "Buzz"
            }
            if (one_based % to_fizz != 0 && one_based % to_buzz != 0) {
                # Just a normal number.
                call stringify_number {
                    input:
                        the_number = one_based
                }
            }
            String result = select_first([fizzbuzz, fizz, buzz, stringify_number.the_string]
        }
        output {
           Array[String] fizzbuzz_results = result
        }
    }
    task stringify_number {
        input {
            Int the_number
        }
        command <<<
            # This is a Bash script.
            # So we should do good Bash script things like stop on errors
            set -e
            # Now print our number as a string
            echo ~{the_number}
        >>>
        output {
            String the_string = read_string(stdout())
        }
        runtime {
            cpu: 1
            memory: "0.5 GB"
            disks: "local-disk 1 SSD"
            docker: "ubuntu:24.04"
        }
    }

Because the ``result`` variable is defined inside a ``scatter``, when we
reference it outside the scatter we see it as being an array.


Running the Workflow
--------------------

Now all that remains is to run the workflow! Make an inputs file to specify the
workflow inputs::

    echo '{"FizzBuzz.item_count": 20}' >fizzbuzz.json

Then run it with Toil. If you are on a Slurm cluster, and you are currently in a shared directory available on all your nodes, you can run::

    toil-wdl-runner --jobStore ./fizzbuzz_store --batchSystem slurm --slurmTime 00:10:00 --caching false --batchLogsDir ./logs fizzbuzz.wdl fizzbuzz.json -o fizzbuzz_out -m fizzbuzz_out.json

If instead you want to run your workflow locally, you can run::

    toil-wdl-runner fizzbuzz.wdl fizzbuzz.json -o fizzbuzz_out -m fizzbuzz_out.json

Next Steps
----------

- Try breaking your workflow up into multiple files and using ``import`` statements.

- Publish your workflow `on Dockstore`_.

- Read up on Toil-specific WDL development considerations in :ref:`devWdl`.

.. _`on Dockstore`: https://docs.dockstore.org/en/stable/getting-started/dockstore-workflows.html
