import random

from math import sqrt


def mean(xs):
    """
    Return the mean value of a sequence of values.

    >>> mean([2,4,4,4,5,5,7,9])
    5.0
    >>> mean([9,10,11,7,13])
    10.0
    >>> mean([1,1,10,19,19])
    10.0
    >>> mean([10,10,10,10,10])
    10.0
    >>> mean([1,"b"])
    Traceback (most recent call last):
      ...
    ValueError: Input can't have non-numeric elements
    >>> mean([])
    Traceback (most recent call last):
      ...
    ValueError: Input can't be empty
    """
    try:
        return sum(xs) / float(len(xs))
    except TypeError:
        raise ValueError("Input can't have non-numeric elements")
    except ZeroDivisionError:
        raise ValueError("Input can't be empty")


def std_dev(xs):
    """
    Returns the standard deviation of the given iterable of numbers.

    From http://rosettacode.org/wiki/Standard_deviation#Python

    An empty list, or a list with non-numeric elements will raise a TypeError.

    >>> std_dev([2,4,4,4,5,5,7,9])
    2.0

    >>> std_dev([9,10,11,7,13])
    2.0

    >>> std_dev([1,1,10,19,19])
    8.049844718999243

    >>> std_dev({1,1,10,19,19}) == std_dev({19,10,1})
    True

    >>> std_dev([10,10,10,10,10])
    0.0

    >>> std_dev([1,"b"])
    Traceback (most recent call last):
    ...
    ValueError: Input can't have non-numeric elements

    >>> std_dev([])
    Traceback (most recent call last):
    ...
    ValueError: Input can't be empty
    """
    m = mean(xs)  # this checks our pre-conditions, too
    return sqrt(sum((x - m) ** 2 for x in xs) / float(len(xs)))


def partition_seq(seq, size):
    """
    Splits a sequence into an iterable of subsequences. All subsequences are of the given size,
    except the last one, which may be smaller. If the input list is modified while the returned
    list is processed, the behavior of the program is undefined.

    :param seq: the list to split
    :param size: the desired size of the sublists, must be > 0
    :type size: int
    :return: an iterable of sublists

    >>> list(partition_seq("",1))
    []
    >>> list(partition_seq("abcde",2))
    ['ab', 'cd', 'e']
    >>> list(partition_seq("abcd",2))
    ['ab', 'cd']
    >>> list(partition_seq("abcde",1))
    ['a', 'b', 'c', 'd', 'e']
    >>> list(partition_seq("abcde",0))
    Traceback (most recent call last):
    ...
    ValueError: Size must be greater than 0
    >>> l=[1,2,3,4]
    >>> i = iter( partition_seq(l,2) )
    >>> l.pop(0)
    1
    >>> i.next()
    [2, 3]
    """
    if size < 1:
        raise ValueError('Size must be greater than 0')
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))


def truncExpBackoff():
    # as recommended here https://forums.aws.amazon.com/thread.jspa?messageID=406788#406788
    # and here https://cloud.google.com/storage/docs/xml-api/reference-status
    yield 0
    t = 1
    while t < 1024:
        # google suggests this dither
        yield t + random.random()
        t *= 2
    while True:
        yield t
