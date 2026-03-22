from teccl.input_data import TopologyParams
from teccl.topologies.topology import Topology


class NVSwitch16(Topology):
    def __init__(self, topo_input: TopologyParams):
        super().__init__(topo_input)
        # Two chassis, each with 8 GPUs.
        self.node_per_chassis = 8

    def construct_topology(self, topo_input: TopologyParams):
        self.node_per_chassis = 8

        # GPU layout:
        #   chassis 0: GPU 0..7
        #   chassis 1: GPU 8..15
        #
        # Modeling choice:
        # - Inside a chassis, all 8 GPUs are fully connected.
        # - Each intra-chassis GPU pair has 8 x 25 GB/s links.
        # - Across chassis, only GPU4 <-> GPU12 is connected with one 25 GB/s link.
        self.topology = [
            # 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15
            [0, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0],  # 0
            [1, 0, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0],  # 1
            [1, 1, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0],  # 2
            [1, 1, 1, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0],  # 3
            [1, 1, 1, 1, 0, 1, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0],  # 4
            [1, 1, 1, 1, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0],  # 5
            [1, 1, 1, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0],  # 6
            [1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0],  # 7
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1],  # 8
            [0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 1, 1, 1, 1],  # 9
            [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 1, 1, 1, 1, 1],  # 10
            [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 1],  # 11
            [0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 1, 0, 1, 1, 1],  # 12
            [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 1, 1],  # 13
            [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 0, 1],  # 14
            [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 0],  # 15
        ]

        local_link_capacity = (8 * 25) / self.chunk_size
        inter_chassis_link_capacity = 100 / self.chunk_size
        local_alpha = topo_input.alpha[0]
        inter_chassis_alpha = topo_input.alpha[1] if len(topo_input.alpha) > 1 else local_alpha

        self.capacity = [[0] * 16 for _ in range(16)]
        self.alpha = [[-1] * 16 for _ in range(16)]

        for src in range(16):
            for dst in range(16):
                if self.topology[src][dst] == 0:
                    continue
                self.capacity[src][dst] = local_link_capacity
                self.alpha[src][dst] = local_alpha

        # The only inter-chassis edge is GPU4 <-> GPU12.
        self.capacity[4][12] = inter_chassis_link_capacity
        self.capacity[12][4] = inter_chassis_link_capacity
        self.alpha[4][12] = inter_chassis_alpha
        self.alpha[12][4] = inter_chassis_alpha

    def set_switch_indicies(self) -> None:
        self.switch_indices = []
