from typing import List, Dict
import xml.etree.ElementTree as ET


def xmltree_to_dict(xml_source: str, is_path: bool = True) -> List[Dict]:
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

    The return is
        [
         {"book_id": "bk101", "book_name": "bookname1", "author":"author1", "price_currency": "USD", "price": "10"},
         {"book_id": "bk102", "book_name": "bookname2", "author":"author2", "price_currency": "USD", "price": "6"}
        ]

    :param xml_source: Xml source
    :param is_path: The xml source is path, otherwise is a xml string
    :return:
    """

    def get_attributes(node: ET.Element) -> Dict:
        """
         Get the node's attributes

        for example <date id="15-11-2023" time="12:34">:
        the return would be: {'date_id': '15-11-2023', 'date_time': '12:34'}

        :param node: Current node
        :return:
        """
        return {f"{node.tag.lower()}_{key.lower()}": value for key, value in node.attrib.items()}

    def merge_attributes_and_value(node: ET.Element, leaf: ET.Element) -> Dict:
        """
         Merge current node's attributes, all the leafs' attributes and text

        :param node: Node
        :param leaf: Leaf
        :return:
        """
        return get_attributes(node) | get_attributes(leaf) | {leaf.tag.lower(): leaf.text}

    def backtracking(node: ET.Element, combination: Dict):
        """
         Generate all the combinations from root to the node closest to leaves based on the backtracking algorithm

        If the node's children are leaves:
            Current recursion ends, merge combination with current node's attributes, children's attributes and text
        else:
            Get the attributes of the current node,
            traverse each child and start a new recursion to generate all the combinations

        :param node: Current node
        :param combination: The combination from root to current node
        :return:
        """

        # when the node's children are leaves
        if len(node) > 0 and len(node[0]) == 0:
            """
            there are two situations:
               1. all the leaves have the same tag name like "book" leaves in the following example
                   <catalog>
                      <book>book_name1</book>
                      <book>book_name2</book>
                   </catalog>
               2. each leaf has different tag name
                   <catalog>
                      <book>book_name1</book>
                      <price>10</price>
                   </catalog>
            """
            is_append = True if len(node.findall(node[0].tag)) > 1 else False
            # all the leaves have the same tag, directly append to combinations
            if is_append:
                for leaf in node:
                    combinations.append(combination | merge_attributes_and_value(node, leaf))
            # each leaf has different tag name, merge all the leaves and append to combinations
            else:
                for leaf in node:
                    combination |= merge_attributes_and_value(node, leaf)
                combinations.append(combination)

        # when the node is far away from leaves
        else:
            for child in node:
                backtracking(child, combination | get_attributes(child))

    combinations = []
    # read xml and get root node
    root = ET.parse(xml_source).getroot() if is_path else ET.fromstring(xml_source)

    if len(root) > 0:
        backtracking(root, get_attributes(root))

    return combinations
