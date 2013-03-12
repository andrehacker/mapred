/*
Example from Pig tutorial
Extracts the first column of the :-separated passwd file

It uses some Grunt commands (not part of Pig Latin)
to prepare the test (load data, etc.)
*/

-- Load input to hdfs (using grunt commands)
rm testpig  -- removes recursively
mkdir testpig
mkdir testpig/input
mkdir testpig/output
copyFromLocal /etc/passwd testpig/input

-- Load data
A = load 'passwd' using PigStorage(':');

-- Transform data
B = foreach A generate $0 as id;

-- Store and show results
store B into 'testpig/output';
dump B;