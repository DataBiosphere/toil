version 1.0

# Test that Cromwell-style Pair inputs ({"Left": x, "Right": y}) are accepted.

workflow cromwell_pair {
    input {
        Pair[Int, Int] p
        Pair[String, Pair[Int, Int]] nested
    }
    output {
        Int left_out = p.left
        Int right_out = p.right
        String nested_left_out = nested.left
        Int nested_inner_left_out = nested.right.left
        Int nested_inner_right_out = nested.right.right
    }
}
