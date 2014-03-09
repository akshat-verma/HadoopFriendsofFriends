This project develops a Hadoop Map Reduce application for recommending friends in a social network based on number of mutual friends
Input file is assumed to be in the following format:
A B,C,D
B A,D,E
C A
D A,B
E B
The first word in each line is name of an individual in the social network followed by a space and then a comma separated list of her friends 
How to Run?
Run Driver.java with input file path as the first arguement output directory as the second argument
The final output file with recommended friends for each individual is the part-r-00000 file inside the outputdirectory