Map Reduce implementation of Regularized Markov Clustering

Input: edge file with src\tdest per line. Each edge should appear only once.

Output: Each line of output is of the form: vertex_id\tcluster_id where cluster_id is the cluster to which vertex_id belongs.

hadoop jar Path to Jar/RMCL.jar RMCL prints command line arguments. All command line arguments are required

To Run:

hadoop jar Path to Jar/RMCL.jar RMCL command line arguments


Note: This code exemplifies vertex centric approach similar to Google's Pregel graph engine and Graphlab's graph analytics toolkit. But since its well known that Hadoop performs poorly when it comes to iterative data mining algorithms (caused by the enormous overhead of scheduling and checkpointing per iteration), not to my surprise this code does take time to run even on moderately sized graphs like LiveJournal network available on the website: snap.stanford.edu
