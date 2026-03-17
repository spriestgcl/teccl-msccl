import argparse
import json
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path


FLOW_RE = re.compile(
    r"Chunk (?P<chunk>\d+) from (?P<source>\d+) traveled over "
    r"(?P<sender>\d+)->(?P<receiver>\d+) in epoch (?P<epoch>\d+)"
)


@dataclass
class Flow:
    chunk: int
    source: int
    sender: int
    receiver: int
    epoch: int


@dataclass(eq=False)
class IROp:
    rank: int
    peer: int
    direction: str
    epoch: int
    offset: int
    count: int
    depends: list = field(default_factory=list)
    tb_id: int = None
    xml_step_idx: int = None
    hasdep: bool = False

    def __hash__(self):
        return id(self)


@dataclass
class XMLStep:
    step_type: str
    srcbuf: str
    srcoff: int
    dstbuf: str
    dstoff: int
    cnt: int
    dep: tuple = None
    hasdep: str = "0"


def indent_xml(elem, level=0):
    indent = "\n" + level * "  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = indent + "  "
        for child in elem:
            indent_xml(child, level + 1)
        if not child.tail or not child.tail.strip():
            child.tail = indent
    elif level and (not elem.tail or not elem.tail.strip()):
        elem.tail = indent


def parse_flows(flow_strings):
    flows = []
    for flow in flow_strings:
        match = FLOW_RE.match(flow)
        if not match:
            raise ValueError(f"Unsupported flow format: {flow}")
        flows.append(Flow(**{k: int(v) for k, v in match.groupdict().items()}))
    return flows


def build_ir(schedule_json):
    flow_strings = schedule_json.get("7-Flows", [])
    if not flow_strings:
        raise ValueError("Input JSON does not contain 7-Flows.")

    flows = parse_flows(flow_strings)
    gpu_ids = sorted(
        {
            gpu
            for flow in flows
            for gpu in (flow.source, flow.sender, flow.receiver)
        }
    )

    if gpu_ids != [1, 2, 3, 4]:
        raise NotImplementedError(
            "This converter currently supports only the A800_4GPU numbering scheme with GPUs 1..4."
        )

    chunk_factor = max(flow.chunk for flow in flows) + 1
    rank_count = len(gpu_ids)
    ops_by_rank = defaultdict(list)

    # producer[(rank, source_rank, chunk_id)] is the recv op that made the chunk
    # available on this rank after a previous epoch. A locally-owned chunk starts
    # available with no dependency anchor.
    producers = {}
    for rank in range(rank_count):
        for chunk in range(chunk_factor):
            producers[(rank, rank, chunk)] = None

    flows_by_epoch = defaultdict(list)
    for flow in flows:
        flows_by_epoch[flow.epoch].append(flow)

    for epoch in sorted(flows_by_epoch):
        new_producers = {}
        epoch_flows = sorted(
            flows_by_epoch[epoch],
            key=lambda flow: (flow.sender, flow.receiver, flow.source, flow.chunk),
        )
        for flow in epoch_flows:
            source_rank = flow.source - 1
            send_rank = flow.sender - 1
            recv_rank = flow.receiver - 1
            offset = source_rank * chunk_factor + flow.chunk
            producer_key = (send_rank, source_rank, flow.chunk)

            if producer_key not in producers:
                raise ValueError(
                    f"GPU {flow.sender} sends chunk {flow.chunk} from GPU {flow.source} "
                    f"in epoch {flow.epoch} before it owns that chunk."
                )

            send_dep = producers[producer_key]
            send_op = IROp(send_rank, recv_rank, "send", epoch, offset, 1)
            if send_dep is not None:
                send_op.depends.append(send_dep)

            recv_op = IROp(recv_rank, send_rank, "recv", epoch, offset, 1)

            ops_by_rank[send_rank].append(send_op)
            ops_by_rank[recv_rank].append(recv_op)
            new_producers[(recv_rank, source_rank, flow.chunk)] = recv_op

        producers.update(new_producers)

    return {
        "rank_count": rank_count,
        "chunk_factor": chunk_factor,
        "output_chunks": rank_count * chunk_factor,
        "ops_by_rank": ops_by_rank,
    }


def assign_threadblocks(ir):
    rank_count = ir["rank_count"]
    ops_by_rank = ir["ops_by_rank"]
    tbs_by_rank = defaultdict(dict)

    for rank in range(rank_count):
        recv_peers = sorted({op.peer for op in ops_by_rank[rank] if op.direction == "recv"})
        send_peers = sorted({op.peer for op in ops_by_rank[rank] if op.direction == "send"})

        next_tb_id = 0
        for peer in recv_peers:
            tbs_by_rank[rank][("recv", peer)] = {"id": next_tb_id, "ops": []}
            next_tb_id += 1
        for peer in send_peers:
            tbs_by_rank[rank][("send", peer)] = {"id": next_tb_id, "ops": []}
            next_tb_id += 1

        for op in sorted(ops_by_rank[rank], key=lambda item: (item.epoch, item.direction, item.peer, item.offset)):
            tb = tbs_by_rank[rank][(op.direction, op.peer)]
            op.tb_id = tb["id"]
            tb["ops"].append(op)

    return tbs_by_rank


def lower_rank_ops(tb_map):
    tb_steps = {}

    for tb_key, tb in tb_map.items():
        ops = sorted(tb["ops"], key=lambda op: (op.epoch, op.offset))

        for op in ops:
            filtered_depends = [dep for dep in op.depends if dep.tb_id != op.tb_id]
            op.depends = filtered_depends
            for dep in op.depends:
                dep.hasdep = True

        steps = []
        for op in ops:
            if len(op.depends) > 1:
                extra_deps = op.depends[1:]
                op.depends = op.depends[:1]
                for dep in extra_deps:
                    steps.append(
                        XMLStep(
                            "nop",
                            "i",
                            -1,
                            "o",
                            -1,
                            0,
                            dep=(dep.tb_id, dep.xml_step_idx),
                            hasdep="0",
                        )
                    )

            op.xml_step_idx = len(steps)
            dep = None
            if op.depends:
                dep = (op.depends[0].tb_id, op.depends[0].xml_step_idx)

            steps.append(
                XMLStep(
                    "r" if op.direction == "recv" else "s",
                    "o",
                    op.offset,
                    "o",
                    op.offset,
                    op.count,
                    dep=dep,
                    hasdep="1" if op.hasdep else "0",
                )
            )

        tb_steps[tb_key] = {"id": tb["id"], "steps": steps}

    return tb_steps


def lower_ir_to_xml(ir):
    rank_count = ir["rank_count"]
    chunk_factor = ir["chunk_factor"]
    output_chunks = ir["output_chunks"]
    tbs_by_rank = assign_threadblocks(ir)

    rank_xml = {}
    for rank in range(rank_count):
        rank_xml[rank] = lower_rank_ops(tbs_by_rank[rank])

    algo = ET.Element(
        "algo",
        {
            "name": "teccl_a800_4gpu_allgather",
            "proto": "Simple",
            "nchannels": "1",
            "nchunksperloop": str(output_chunks),
            "ngpus": str(rank_count),
            "coll": "allgather",
            "inplace": "1",
            "outofplace": "0",
            "minBytes": "1",
            "maxBytes": str(1 << 60),
        },
    )

    for rank in range(rank_count):
        gpu = ET.SubElement(
            algo,
            "gpu",
            {
                "id": str(rank),
                "i_chunks": str(chunk_factor),
                "o_chunks": str(output_chunks),
                "s_chunks": "0",
            },
        )

        ordered_tb_keys = sorted(
            rank_xml[rank].keys(),
            key=lambda key: (0 if key[0] == "recv" else 1, key[1]),
        )
        for direction, peer in ordered_tb_keys:
            tb_meta = rank_xml[rank][(direction, peer)]
            tb_attrs = {
                "id": str(tb_meta["id"]),
                "send": str(peer) if direction == "send" else "-1",
                "recv": str(peer) if direction == "recv" else "-1",
                "chan": "0",
            }
            tb = ET.SubElement(gpu, "tb", tb_attrs)

            for idx, step in enumerate(tb_meta["steps"]):
                depid = str(step.dep[0]) if step.dep is not None else "-1"
                deps = str(step.dep[1]) if step.dep is not None else "-1"
                ET.SubElement(
                    tb,
                    "step",
                    {
                        "s": str(idx),
                        "type": step.step_type,
                        "srcbuf": step.srcbuf,
                        "srcoff": str(step.srcoff),
                        "dstbuf": step.dstbuf,
                        "dstoff": str(step.dstoff),
                        "cnt": str(step.cnt),
                        "depid": depid,
                        "deps": deps,
                        "hasdep": step.hasdep,
                    },
                )

    indent_xml(algo)
    return algo


def build_msccl_xml(schedule_json):
    ir = build_ir(schedule_json)
    return lower_ir_to_xml(ir)


def main():
    parser = argparse.ArgumentParser(
        description="Convert a TE-CCL schedule JSON into an MSCCL XML for A800_4GPU AllGather."
    )
    parser.add_argument("-i", "--input", required=True, help="Path to TE-CCL output JSON.")
    parser.add_argument("-o", "--output", help="Path to output XML. Defaults to input path with .xml suffix.")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else input_path.with_suffix(".xml")

    with open(input_path, "r") as f:
        schedule_json = json.load(f)

    algo = build_msccl_xml(schedule_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(algo).write(output_path, encoding="utf-8", xml_declaration=False)
    print(f"MSCCL XML written to {output_path}")


if __name__ == "__main__":
    main()
