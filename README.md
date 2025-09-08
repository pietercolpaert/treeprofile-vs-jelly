# TREE/LDES profile vs. Jelly

When fetching data over the web, we can either use the standard RDF serializations, or use specialized RDF serializations to speed up performance.

If the client wants to process groups of triples together, standard RDF serializations have a lot of overhead, as first you need to parse the entire page before being able to use the first group. Therefore we introduced the [TREE profile](https://treecg.github.io/specification/profile) that promises to the client that certain quads will be grouped so they can be processed earlier. In combination with gzip, we thought that would be a good trade-off in performance and bandwidth consumption.

[Jelly RDF](https://jelly-rdf.github.io/) is gaining traction. It is a binary format for storing RDF quads that has native support for grouping quads into “frames”. On their own webpage they produce impressive charts on performance to serialize and parse.

I wonder if when we apply it in an LDES setting, what the speed up would be: would it indeed be a huge as promised on their website, or would the benefit be negligible? I was skeptical and wanted to feel the difference.


## Coding up an experiment in Python

As I’m not a python dev and there’s currently no support for Jelly in Javascript,
I used this prompt:

```
I want to create an experiment in which I test the difference in performance between parsing the Jelly RDF serialization for 10000 LDES members in one page, and compare it with a gzipped TREE profile algorithm approach containing the same amount of members. Can you provide

the code to create 10000 LDES members with varying triple counts in the TREE profile algorithm (https://raw.githubusercontent.com/TREEcg/specification/refs/heads/master/05-profile-specification.bs)
write the code to convert the data into Jelly RDF
write the code that measures the time in which each time 100 members are parsed from the page into a set of quads
```

## Running the experiment

Installing dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install rdflib pyjelly[rdflib]==0.6.1
```

Running: `python3 ldes_tree_vs_jelly_benchmark.py`

We’ve already included the data as well in this repository, but when running the script, it will regenerate a 10000 LDES member large dataset in both the TREE profile as in a Jelly format.

## Results

### Throughput test
```bash
=== TREE profile (.tree.nq.gz) parsing ===
Total members processed in batches: 10000
Total quads processed in batches:   179089
Sum of batch times:                 0.4375s
Throughput (members/s):            22855.30
Throughput (quads/s):              409313.27
Batch 0: members=100, quads=1832, time=0.0063s
Batch 1: members=100, quads=1768, time=0.0045s
Batch 2: members=100, quads=1820, time=0.0043s

=== Jelly parsing ===
Initial load time (not batched): 7.6948s
Total members processed in batches: 10001
Total quads processed in batches:   179102
Sum of batch times:                 0.0243s
Throughput (members/s):            411473.32
Throughput (quads/s):              7368832.60
Batch 0: members=100, quads=1808, time=0.0003s
Batch 1: members=100, quads=1909, time=0.0003s
Batch 2: members=100, quads=1892, time=0.0003s
```

### Disk space

```bash
3,9M    out/dataset.jelly.gz
8,1M    out/dataset.jelly
1,8M    out/tree-page.tree.nq.gz
26M     out/tree-page.tree.nq
```

## Conclusion

Jelly blows the TREE profile out of the water and instead of promoting the TREE profile algorithm, we should look into including text on Jelly in the spec instead.
