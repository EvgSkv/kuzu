from .types import Type
import warnings


class TorchGeometricResultConverter:
    def __init__(self, query_result):
        self.query_result = query_result
        self.nodes_dict = {}
        self.edges_dict = {}
        self.rels = set()
        self.nodes_property_names_dict = {}
        self.table_to_label_dict = {}
        self.internal_id_to_pos_dict = {}
        self.pos_to_primary_key_dict = {}
        self.warning_messages = set()
        self.ignored_properties = set()
        self.properties_to_extract = self.query_result._get_properties_to_extract()

    def __get_node_property_names(self, table_name):
        if table_name in self.nodes_property_names_dict:
            return self.nodes_property_names_dict[table_name]

        PRIMARY_KEY_SYMBOL = "(PRIMARY KEY)"
        LIST_SYMBOL = "[]"
        result_str = self.query_result.connection._connection.get_node_property_names(
            table_name)
        results = {}
        for (i, line) in enumerate(result_str.splitlines()):
            # ignore first line
            if i == 0:
                continue
            line = line.strip()
            if line == "":
                continue
            line_splited = line.split(" ")
            if len(line_splited) < 2:
                continue

            prop_name = line_splited[0]
            prop_type = " ".join(line_splited[1:])

            is_primary_key = PRIMARY_KEY_SYMBOL in prop_type
            prop_type = prop_type.replace(PRIMARY_KEY_SYMBOL, "")
            dimension = prop_type.count(LIST_SYMBOL)
            prop_type = prop_type.replace(LIST_SYMBOL, "")
            results[prop_name] = {
                "type": prop_type,
                "dimension": dimension,
                "is_primary_key": is_primary_key
            }
        self.nodes_property_names_dict[table_name] = results
        return results

    def __populate_nodes_dict_and_deduplicte_edges(self):
        self.query_result.reset_iterator()
        while self.query_result.has_next():
            row = self.query_result.get_next()
            for i in self.properties_to_extract:
                column_type, _ = self.properties_to_extract[i]
                if column_type == Type.NODE.value:
                    node = row[i]
                    label = node["_label"]
                    _id = node["_id"]
                    self.table_to_label_dict[_id["table"]] = label

                    if (_id["table"], _id["offset"]) in self.internal_id_to_pos_dict:
                        continue

                    node_property_names = self.__get_node_property_names(
                        label)

                    pos, primary_key = self.__extract_properties_from_node(
                        node, label, node_property_names)

                    # If no properties were extracted, then ignore the node
                    if pos >= 0:
                        self.internal_id_to_pos_dict[(
                            _id["table"], _id["offset"])] = pos
                        if label not in self.pos_to_primary_key_dict:
                            self.pos_to_primary_key_dict[label] = {}
                        self.pos_to_primary_key_dict[label][pos] = primary_key

                elif column_type == Type.REL.value:
                    _src = row[i]["_src"]
                    _dst = row[i]["_dst"]
                    self.rels.add((_src["table"], _src["offset"],
                                   _dst["table"], _dst["offset"]))

    def __extract_properties_from_node(self, node, label, node_property_names):
        import torch
        for prop_name in node_property_names:
            # Ignore properties that are marked as ignored
            if (label, prop_name) in self.ignored_properties:
                continue

            # Read primary key but do not add it to the node properties
            if node_property_names[prop_name]["is_primary_key"]:
                primary_key = node[prop_name]
                continue

            # Ignore properties that are not supported by torch_geometric
            if node_property_names[prop_name]["type"] not in [Type.INT64.value, Type.DOUBLE.value, Type.BOOL.value]:
                self.warning_messages.add(
                    "Property {}.{} of type {} is not supported by torch_geometric. The property is ignored."
                    .format(label, prop_name, node_property_names[prop_name]["type"]))
                self.__ignore_property(label, prop_name)
                continue
            if node[prop_name] is None:
                self.warning_messages.add(
                    "Property {}.{} has a null value. torch_geometric does not support null values. The property is ignored."
                    .format(label, prop_name))
                self.__ignore_property(label, prop_name)
                continue

            if node_property_names[prop_name]['dimension'] == 0:
                curr_value = node[prop_name]
            else:
                try:
                    if node_property_names[prop_name]['type'] == Type.INT64.value:
                        curr_value = torch.LongTensor(node[prop_name])
                    elif node_property_names[prop_name]['type'] == Type.DOUBLE.value:
                        curr_value = torch.FloatTensor(node[prop_name])
                    elif node_property_names[prop_name]['type'] == Type.BOOL.value:
                        curr_value = torch.BoolTensor(node[prop_name])
                except ValueError:
                    self.warning_messages.add(
                        "Property {}.{} cannot be converted to Tensor (likely due to nested list of variable length). The property is ignored."
                        .format(label, prop_name))
                    self.__ignore_property(label, prop_name)
                    continue
                # Check if the shape of the property is consistent
                if label in self.nodes_dict and prop_name in self.nodes_dict[label]:
                    # If the shape is inconsistent, then ignore the property
                    if curr_value.shape != self.nodes_dict[label][prop_name][0].shape:
                        self.warning_messages.add(
                            "Property {}.{} has an inconsistent shape. The property is ignored."
                            .format(label, prop_name))
                        self.__ignore_property(label, prop_name)
                        continue

            # Create the dictionary for the label if it does not exist
            if label not in self.nodes_dict:
                self.nodes_dict[label] = {}
            if prop_name not in self.nodes_dict[label]:
                self.nodes_dict[label][prop_name] = []

            # Add the property to the dictionary
            self.nodes_dict[label][prop_name].append(curr_value)

            # The pos will be overwritten for each property, but
            # it should be the same for all properties
            pos = len(self.nodes_dict[label][prop_name]) - 1
        return pos, primary_key

    def __ignore_property(self, label, prop_name):
        self.ignored_properties.add((label, prop_name))
        if label in self.nodes_dict and prop_name in self.nodes_dict[label]:
            del self.nodes_dict[label][prop_name]
            if len(self.nodes_dict[label]) == 0:
                del self.nodes_dict[label]

    def __populate_edges_dict(self):
        # Post-process edges, map internal ids to positions
        for r in self.rels:
            src_pos = self.internal_id_to_pos_dict[(r[0], r[1])]
            dst_pos = self.internal_id_to_pos_dict[(r[2], r[3])]
            src_label = self.table_to_label_dict[r[0]]
            dst_label = self.table_to_label_dict[r[2]]
            if src_label not in self.edges_dict:
                self.edges_dict[src_label] = {}
            if dst_label not in self.edges_dict[src_label]:
                self.edges_dict[src_label][dst_label] = []
            self.edges_dict[src_label][dst_label].append((src_pos, dst_pos))

    def __print_warnings(self):
        for message in self.warning_messages:
            warnings.warn(message)

    def __convert_to_torch_geometric(self):
        import torch
        import torch_geometric
        if len(self.nodes_dict) == 0:
            self.warning_messages.add(
                "No nodes found or all nodes were ignored. Returning None.")
            return None

        # If there is only one node type, then convert to torch_geometric.data.Data
        # Otherwise, convert to torch_geometric.data.HeteroData
        if len(self.nodes_dict) == 1:
            data = torch_geometric.data.Data()
            is_hetero = False
        else:
            data = torch_geometric.data.HeteroData()
            is_hetero = True

        # Convert nodes to tensors
        for label in self.nodes_dict:
            for prop_name in self.nodes_dict[label]:
                prop_type = self.nodes_property_names_dict[label][prop_name]["type"]
                prop_dimension = self.nodes_property_names_dict[label][prop_name]["dimension"]
                if prop_dimension == 0:
                    if prop_type == Type.INT64.value:
                        converted = torch.LongTensor(
                            self.nodes_dict[label][prop_name])
                    elif prop_type == Type.BOOL.value:
                        converted = torch.BoolTensor(
                            self.nodes_dict[label][prop_name])
                    elif prop_type == Type.DOUBLE.value:
                        converted = torch.FloatTensor(
                            self.nodes_dict[label][prop_name])
                else:
                    converted = torch.stack(
                        self.nodes_dict[label][prop_name], dim=0)
                if is_hetero:
                    data[label][prop_name] = converted
                else:
                    data[prop_name] = converted

        # Convert edges to tensors
        for src_label in self.edges_dict:
            for dst_label in self.edges_dict[src_label]:
                edge_idx = torch.tensor(
                    self.edges_dict[src_label][dst_label], dtype=torch.long).t().contiguous()
                if is_hetero:
                    data[src_label, dst_label].edge_index = edge_idx
                else:
                    data.edge_index = edge_idx
        pos_to_primary_key_dict = self.pos_to_primary_key_dict[
            label] if not is_hetero else self.pos_to_primary_key_dict
        return data, pos_to_primary_key_dict

    def get_as_torch_geometric(self):
        self.__populate_nodes_dict_and_deduplicte_edges()
        self.__populate_edges_dict()
        data, pos_to_primary_key_dict = self.__convert_to_torch_geometric()
        self.__print_warnings()
        return data, pos_to_primary_key_dict
