# Author: John Wang
# Date: 2/15/2012
#
# Description: Objects for creating a parse tree. Stores strings
# by the first element and does insert and find operations in O(n) 
# time where n is the length of word to be inserted or found.

class Node(object):

    def __init__(self, key, value=None):
        self.key = key
        self.children = []
        self.value = value

    def insert_child(self, key):
        new_node = Node(key, None)
        notfound = True
        for child in self.children:
            if child.key == key:
                notfound = False 
                return child
        if notfound:
            self.children.append(new_node)
        return new_node

    def find_child(self, key):
        for child in self.children:
            if child.key == key:
                return child
        return None

class ParseTree(object):

    def __init__(self, root=""):
        self.root = Node(root, None)
    
    def insert(self, key, value):
        self._insert(self.root, key, value)

    def _insert(self, node, key, value):
        first_letter = key[0]
        new_node = node.insert_child(first_letter)
        if len(key) > 1:
            new_key = key[1:]
            self._insert(new_node, new_key, value)
        else:
            new_node.value = value

    def find(self, key):
        new_node = self.root
        for i in xrange(len(key)):
            letter = key[i]
            new_node = new_node.find_child(letter)
            if new_node == None:
                return None
        return new_node

    def find_value(self, key):
        if self.find(key) == None:
            return None
        else:
            return self.find(key).value



