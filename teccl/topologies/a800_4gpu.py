from teccl.input_data import TopologyParams
from teccl.topologies.topology import Topology


class A800_4GPU(Topology):
    def __init__(self, topo_input: TopologyParams):
        super().__init__(topo_input)
        self.node_per_chassis = 4

    def construct_topology(self, topo_input: TopologyParams):
        # Model a single NVSwitch connected to 4 GPUs. Each GPU has 8 links at
        # 25 GB/s, so we aggregate them into one logical bidirectional edge
        # with 200 GB/s total bandwidth.
        self.node_per_chassis = 4
        aggregated_bandwidth = 8 * 25
        link_capacity = aggregated_bandwidth / self.chunk_size

        # Keep alpha inside the topology definition, mirroring the existing
        # DGX2/NDv2 examples.
        alpha_value = 0.35 * pow(10, -6)

        # Node 0 is the NVSwitch. Nodes 1..4 are GPUs.
        self.capacity = [
            [0, link_capacity, link_capacity, link_capacity, link_capacity],
            [link_capacity, 0, 0, 0, 0],
            [link_capacity, 0, 0, 0, 0],
            [link_capacity, 0, 0, 0, 0],
            [link_capacity, 0, 0, 0, 0],
        ]

        self.alpha = []
        for row in self.capacity:
            alpha_row = []
            for capacity in row:
                if capacity > 0:
                    alpha_row.append(alpha_value)
                else:
                    alpha_row.append(-1)
            self.alpha.append(alpha_row)

    def set_switch_indicies(self) -> None:
        self.switch_indices = [0]
