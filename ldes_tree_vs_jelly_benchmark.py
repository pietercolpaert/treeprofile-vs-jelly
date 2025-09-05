#!/usr/bin/env python3
# file: ldes_tree_vs_jelly_benchmark.py

import gzip
import os
import random
import string
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple
from rdflib import Dataset, Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, XSD

# ----------------------------
# Config
# ----------------------------
OUT_DIR = "out"
BASE = "https://example.org/ldes/"
COLL = URIRef(BASE + "collection")
VIEW = URIRef(BASE + "page/0")
TREE = Namespace("https://w3id.org/tree/")
EX = Namespace("https://example.org/vocab/")
MEM_BASE = BASE + "member/"

NUM_MEMBERS = 10_000
TRIPLES_PER_MEMBER_MIN = 6     # vary as you like
TRIPLES_PER_MEMBER_MAX = 30
RANDOM_SEED = 42

BATCH_SIZE = 100

# ----------------------------
# Helpers
# ----------------------------

def rand_label(n=8) -> str:
    return ''.join(random.choices(string.ascii_letters, k=n))

def make_member_iri(i: int) -> URIRef:
    return URIRef(f"{MEM_BASE}{i:05d}")

def member_quads(member: URIRef, n_triples: int) -> List[Tuple[URIRef, URIRef, object, URIRef]]:
    """
    Create 'n_triples' quads for a member, all in the named graph = member IRI.
    We vary predicate/object a bit to avoid being too repetitive.
    """
    quads = []
    # canonical root triple
    quads.append((member, RDF.type, EX.Member, member))
    quads.append((member, RDFS.label, Literal(f"Member {str(member).split('/')[-1]}"), member))
    quads.append((member, EX.value, Literal(random.randint(0, 1_000_000), datatype=XSD.integer), member))

    # extra triples
    for _ in range(max(0, n_triples - 3)):
        pred = URIRef(f"{EX}{random.choice(['tag','attr','prop','rel'])}")
        # mix literal and IRI objects
        if random.random() < 0.5:
            obj = Literal(rand_label(10))
        else:
            obj = URIRef(BASE + "res/" + rand_label(6))
        quads.append((member, pred, obj, member))
    return quads

def write_tree_profile_page_gz(quads_by_member: Dict[URIRef, List[Tuple[URIRef, URIRef, object, URIRef]]],
                               path_gz: str) -> None:
    """
    Write a single gzipped N-Quads file laid out to follow the TREE profile algorithm rules:
    - Hypermedia block first (<> ... tree:view, tree:relation, etc.)
    - Then for each member: a 'tree:member <memberIRI> .' marker, followed immediately by that member's quads.
    - Each quad is emitted as N-Quads using the member IRI as graph name.
    """
    os.makedirs(os.path.dirname(path_gz), exist_ok=True)
    with gzip.open(path_gz, "wt", encoding="utf-8", newline="\n") as f:
        # Hypermedia block (very small, just enough to satisfy the idea)
        f.write(f"<> <{RDF.type}> <{TREE.Node}> .\n")
        f.write(f"<{COLL}> <{RDF.type}> <{TREE.Collection}> .\n")
        f.write(f"<{COLL}> <{TREE.view}> <> .\n")
        # minimal relation example
        f.write(f"<> <{TREE.relation}> _:r1 .\n")
        f.write(f"_:r1 <{RDF.type}> <{TREE.GreaterThanOrEqualToRelation}> .\n")
        f.write(f"_:r1 <{TREE.node}> <{BASE}page/1> .\n")
        f.write(f"_:r1 <{TREE.value}> \"0\"^^<{XSD.integer}> .\n")
        f.write(f"_:r1 <{TREE.path}> <{EX.value}> .\n")

        # Members (profile algorithm grouping)
        # From the moment tree:member is used, a new member bundle starts.
        for m in quads_by_member.keys():
            f.write(f"<{COLL}> <{TREE.member}> <{m}> .\n")
            for (s, p, o, g) in quads_by_member[m]:
                s_str = term_to_nq(s)
                p_str = term_to_nq(p)
                o_str = term_to_nq(o)
                g_str = term_to_nq(g)
                f.write(f"{s_str} {p_str} {o_str} {g_str} .\n")

def term_to_nq(t) -> str:
    if isinstance(t, URIRef):
        return f"<{t}>"
    if isinstance(t, Literal):
        # keep it simple: only plain and xsd:integer in our generator
        if t.datatype == XSD.integer:
            return f"\"{int(t)}\"^^<{XSD.integer}>"
        if t.language:
            return f"\"{t}\"@{t.language}"
        return f"\"{t}\""
    # rdflib blank nodes won't appear in our generated dataset for member quads
    raise ValueError(f"Unsupported term type in this generator: {t!r}")

def serialize_to_jelly(quads_by_member, path_jelly: str) -> None:
    os.makedirs(os.path.dirname(path_jelly), exist_ok=True)
    ds = Dataset()
    for member, quads in quads_by_member.items():
        g = ds.graph(member)
        for (s, p, o, _g) in quads:
            g.add((s, p, o))
    # Write gzipped Jelly (open in binary mode)
    with gzip.open(path_jelly, "wb") as f:
        ds.serialize(destination=f, format="jelly")

# ----------------------------
# Benchmarking
# ----------------------------

@dataclass
class BatchStat:
    batch_index: int
    members_in_batch: int
    quads_in_batch: int
    seconds: float

def parse_tree_profile_batches(path_gz: str, batch_size: int) -> List[BatchStat]:
    """
    Streaming profile parser (order-preserving) over our own .tree.nq.gz file:
    - Scan lines
    - When encountering '<COLL> tree:member <memberIri> .', start a new member bundle
    - Collect subsequent N-Quads into that member until the next tree:member or EOF
    - Every time we reach 'batch_size' members, record elapsed time and reset counters
    Returns list of batch stats.
    """
    batch_stats: List[BatchStat] = []
    batch_index = 0
    current_members = 0
    current_quads = 0

    t0_batch = None
    in_members = False
    # simplistic line parsing (we control the output format)
    current_member = None

    def flush_batch():
        nonlocal batch_index, current_members, current_quads, t0_batch
        if current_members == 0:
            return
        dt = time.perf_counter() - t0_batch
        batch_stats.append(BatchStat(batch_index, current_members, current_quads, dt))
        batch_index += 1
        current_members = 0
        current_quads = 0
        t0_batch = None

    with gzip.open(path_gz, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # detect start of member group
            if line.startswith(f"<{COLL}> <{TREE.member}> <"):
                # close previous member (no-op; we only count)
                current_member = line.split(" ", 2)[2]
                # start batch timing when first member in batch arrives
                if t0_batch is None:
                    t0_batch = time.perf_counter()
                in_members = True
                current_members += 1

                if current_members == batch_size:
                    # batch complete; we flush and continue
                    flush_batch()
                continue

            # count quads only when inside member bundles
            if in_members and line.endswith(" ."):
                # ignore hypermedia lines that could appear if a producer interleaves (we don't)
                # but to be safe: treat any non-marker line as a quad
                current_quads += 1

    # flush last (possibly partial) batch
    flush_batch()
    return batch_stats

def parse_jelly_batches(path_jelly: str, batch_size: int):
    ds = Dataset()
    # Load gzipped Jelly
    t0_load = time.perf_counter()
    with gzip.open(path_jelly, "rt", encoding="utf-8") as f:
        ds.parse(f, format="jelly")
    load_time = time.perf_counter() - t0_load

    batch_stats = []
    batch_index = 0
    current_members = 0
    current_quads = 0
    t0_batch = None

    # Each context is a named graph (one member)
    for g in ds.contexts():
        if t0_batch is None:
            t0_batch = time.perf_counter()

        # Option A (fast & simple):
        q = len(g)

        # Option B (more explicit, similar perf):
        # q = sum(1 for _ in g.triples((None, None, None)))

        current_members += 1
        current_quads += q

        if current_members == batch_size:
            dt = time.perf_counter() - t0_batch
            batch_stats.append(BatchStat(batch_index, current_members, current_quads, dt))
            batch_index += 1
            current_members = 0
            current_quads = 0
            t0_batch = None

    # Flush any leftover members in the final partial batch
    if current_members > 0:
        dt = time.perf_counter() - t0_batch
        batch_stats.append(BatchStat(batch_index, current_members, current_quads, dt))

    # Prepend a “batch -1” record so you can see the one-off Jelly load cost
    batch_stats.insert(0, BatchStat(-1, 0, 0, load_time))
    return batch_stats
# ----------------------------
# Main
# ----------------------------

def main():
    random.seed(RANDOM_SEED)
    os.makedirs(OUT_DIR, exist_ok=True)

    # 1) Generate members/quads
    quads_by_member: Dict[URIRef, List[Tuple[URIRef, URIRef, object, URIRef]]] = {}
    for i in range(NUM_MEMBERS):
        m = make_member_iri(i)
        n = random.randint(TRIPLES_PER_MEMBER_MIN, TRIPLES_PER_MEMBER_MAX)
        quads_by_member[m] = member_quads(m, n)

    # 2) Write TREE profile page (gzipped N-Quads with profile bundling)
    tree_path = os.path.join(OUT_DIR, "tree-page.tree.nq.gz")
    write_tree_profile_page_gz(quads_by_member, tree_path)

    # 3) Convert same dataset to Jelly
    jelly_path = os.path.join(OUT_DIR, "dataset.jelly.gz")
    serialize_to_jelly(quads_by_member, jelly_path)

    # 4) Benchmark: TREE profile parsing (streaming, batches of 100)
    tree_stats = parse_tree_profile_batches(tree_path, BATCH_SIZE)

    # 5) Benchmark: Jelly parsing (load file to dataset, then iterate contexts in 100s)
    jelly_stats = parse_jelly_batches(jelly_path, BATCH_SIZE)

    # 6) Print a compact report
    def summarize(label: str, stats: List[BatchStat]):
        print(f"\n=== {label} ===")
        # If first record has batch_index = -1, treat as upfront load time info
        offset = 0
        if stats and stats[0].batch_index == -1:
            print(f"Initial load time (not batched): {stats[0].seconds:.4f}s")
            offset = 1
        if len(stats) > offset:
            total_members = sum(s.members_in_batch for s in stats[offset:])
            total_quads = sum(s.quads_in_batch for s in stats[offset:])
            total_time = sum(s.seconds for s in stats[offset:])
            print(f"Total members processed in batches: {total_members}")
            print(f"Total quads processed in batches:   {total_quads}")
            print(f"Sum of batch times:                 {total_time:.4f}s")
            if total_time > 0:
                print(f"Throughput (members/s):            {total_members/total_time:.2f}")
                print(f"Throughput (quads/s):              {total_quads/total_time:.2f}")
        # Show first 3 batches as sample
        for s in stats[offset:offset+3]:
            print(f"Batch {s.batch_index}: members={s.members_in_batch}, quads={s.quads_in_batch}, time={s.seconds:.4f}s")

    summarize("TREE profile (.tree.nq.gz) parsing", tree_stats)
    summarize("Jelly parsing", jelly_stats)

if __name__ == "__main__":
    main()

