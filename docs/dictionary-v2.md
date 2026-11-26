# V2 graph manifest: declarable external vertex dictionary

Format 2 adds an explicit **external vertex dictionary** so edge records can be
transmitted as compact labels instead of full 64-bit external IDs. The
dictionary is transport-only: after labels resolve to external IDs, the same
canonical `Graph` (ascending external dense order, FNV-1a identity, external-ID
placement) is built as for a v1 manifest. Dictionary declaration order therefore
never changes identity, partitions or computed results; only the label mapping
matters.

## Canonical form emitted by `write_graph_v2` / `canonicalize --compact`

```text
FRONTIERLOOM_GRAPH 2
revision REVISION
dictionary VERTEX_COUNT
EXTERNAL_ID            # positional label 0
EXTERNAL_ID            # positional label 1
...
edges EDGE_COUNT
COMPACT_LABEL COMPACT_LABEL
...
end
```

The writer lists ascending external IDs, so positional compact labels equal the
canonical dense vertex indices. `save_graph` / default `canonicalize` still emit
byte-stable v1.

## Accepted declaration dialects

The reader (`read_graph_v2`, and `load_graph` after version sniffing) accepts:

- `dictionary N` / `dict N` / `labels N` followed by `N` positional IDs;
- `N` times explicit `LABEL EXTERNAL_ID` pairs, in either column orientation;
- text labels (`a`, `campus`, ...) or numeric labels;
- keyed entries `LABEL=ID`, `LABEL:ID` or `LABEL,ID`;
- a bare pair list without a count;
- the legacy `vertices N` block, whose order acts as the positional dictionary
  for zero-based compact edge labels (full external-ID edge endpoints are also
  accepted when they name declared vertices).

Labels may be referenced literally, by numerically normalized spelling (`00`),
or with one leading `#`/`@`/`$` marker.

## External dictionary file

`--dictionary FILE` (aliases `--dictionary-file`, `--label-map`, `--labels`)
supplies the label map for a compact graph that does not declare one inline:

```text
dictionary 3
a 10
b 20
c 30
```

Inline declarations take precedence over the external map.

## Completeness and publication

Every edge endpoint must resolve through the dictionary. An unresolved label
fails with an `invalid_argument` whose message contains
`incomplete dictionary references`, `unresolved label` and `dictionary`.
Loading happens before the checkpoint `Store` is opened, so a compute/resume
invocation with an incomplete dictionary exits `2` and neither creates nor
overwrites a published checkpoint.

## Compatibility

- `FRONTIERLOOM_GRAPH 1` parsing and output are unchanged and byte-stable.
- Graph identity is the canonical v1 FNV-1a over revision, sorted external IDs
  and sorted external endpoint IDs, so a v2 dictionary graph has the same
  identity as its v1 twin and reopens the checked-in v1 snapshot
  (`samples/campus.epoch1.v1.flc`).
- Permuting dictionary records (with their label/ID bindings) yields the same
  identity, shards, BFS/PageRank output vectors and checkpoint compatibility.
