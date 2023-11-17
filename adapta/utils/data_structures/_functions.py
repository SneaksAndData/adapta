"""
 Module for data structures methods.
"""
import os.path
from pathlib import Path
from typing import List, Union, TypeVar, Dict
import xml.etree.ElementTree as ET

XmlNodeT = TypeVar("XmlNodeT")


def xmltree_to_dict_collection(xml_source: Union[str, Path], node_type: type[XmlNodeT]) -> List[XmlNodeT]:
    """
     Convert a xml source to a list of dict, which can be a path or a xml string

    for example
        <?xml version="1.0"?>
        <catalog>
           <book id="bk101" name="bookname1">
              <author>author1</author>
              <price currency="USD">10</price>
           </book>
           <book id="bk102" name="bookname2">
              <author>author2</author>
              <price currency="USD">6</price>
           </book>
        </catalog>

    When node_type is dict, the returned value is
        [
         {"book_id": "bk101", "book_name": "bookname1", "author":"author1", "price_currency": "USD", "price": "10"},
         {"book_id": "bk102", "book_name": "bookname2", "author":"author2", "price_currency": "USD", "price": "6"}
        ]

    :param xml_source: Valid XML string or a path to a valid xml file
    :param node_type: The type of each element in returned List, like dict or a created class inheriting from DataClassJsonMixin
    :return:
    """

    def node_attributes_to_dict(node: ET.Element) -> Dict:
        """
         Get the node's attributes

        for example <date id="15-11-2023" time="12:34">:
        the return would be: {'date_id': '15-11-2023', 'date_time': '12:34'}

        :param node: Current node
        :return:
        """
        return {f"{node.tag.lower()}_{key.lower()}": value for key, value in node.attrib.items()}

    def merge(node: ET.Element, leaf: ET.Element) -> Dict:
        """
         Merge current node's attributes, all the leafs' attributes and text

        :param node: Node
        :param leaf: Leaf
        :return:
        """
        assert len(leaf) == 0, "Sub-element detected, the expectation is each leaf node should not have sub-tag."

        return node_attributes_to_dict(node) | node_attributes_to_dict(leaf) | {leaf.tag.lower(): leaf.text}

    def node_type_convert(base_node: Dict) -> XmlNodeT:
        """
         Convert type of node to XmlNodeT

        :param base_node: Node to be converted to XmlNodeT
        :return:
        """
        return base_node if node_type is dict else node_type.from_dict(base_node)

    def backtrack(node: ET.Element, converted_node: Dict):
        """
         Generate all the combinations from root to the node closest to leaves based on the backtracking algorithm

        Base case (reached leaf nodes), there are two possible outcomes:
               1. all the leaves have the same tag name like "book" leaves in the following example
                   <catalog>
                      <book>book_name1</book>
                      <book>book_name2</book>
                   </catalog>
                Then directly append to combinations

               2. each leaf has different tag name
                   <catalog>
                      <book>book_name1</book>
                      <price>10</price>
                   </catalog>
                Then merge all the leaves and append to combinations

        Recursive case:
            Get the attributes of the current node,
            traverse each child and start a new recursion to generate all the combinations

        :param node: Current node
        :param converted_node: The combination from root to current node
        :return:
        """

        # when the node's children are leaves
        if len(node) > 0 and len(node[0]) == 0:
            # all the leaves have the same tag, directly append to combinations
            if len(node.findall(node[0].tag)) > 1:
                for leaf in node:
                    converted_nodes.append(node_type_convert(converted_node | merge(node, leaf)))
            # each leaf has different tag name, merge all the leaves and append to combinations
            else:
                for leaf in node:
                    converted_node |= merge(node, leaf)

                converted_nodes.append(node_type_convert(converted_node))

        # when the node is far away from leaves
        else:
            for child in node:
                backtrack(child, converted_node | node_attributes_to_dict(child))

    if isinstance(xml_source, Path) and not os.path.isfile(xml_source):
        raise RuntimeError("Provided path is not a file or does not exist")

    converted_nodes: list[XmlNodeT] = []
    # read xml and get root node
    root = ET.parse(str(xml_source)).getroot() if isinstance(xml_source, Path) else ET.fromstring(xml_source)

    if len(root) > 0:
        backtrack(root, node_attributes_to_dict(root))

    return converted_nodes
