matrix = [[1, 2, 3], [4, 5], [6, 7, 8, 9]]

# Nested List Comprehension to flatten a given 2-D matrix 
flatten_matrix = [val for sublist in matrix for val in sublist]

print(flatten_matrix)

matrix_2 = [[[1, 2, 3], [4, 5], [6, 7, 8, 9]], [[1, 2, 3], [4, 5], [6, 7, 8, 9]]]


flatten_matrix_2 = [v for sublist in matrix_2 for val in sublist for v in val]

print(flatten_matrix_2)
