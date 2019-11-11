"""
cymod.cybase
~~~~~~~~~~~~

This module contains classes used to customise features of cymod to tailor it 
to particular subject domains.
"""


class NodeLabels(object):
    """Contains data necessary to cusomise transition table node labels.
    
    By default, if a transition table is used to load model data into a 
    graph, using e.g. :obj:`GraphLoader.load_tabular`, all state nodes will be
    labelled 'State', nodes representing transitions between states will be
    labelled 'Transition', and sets of conditions which cause transitions will
    be labelled 'Condition'. As one of the objectives of cymod is to reduce the 
    gap between the modeller's ideas and the visual representation of their 
    model, we wish to make these customisable. This class facilitates that
    customisation.
    """

    def __init__(self, label_map=None):
        """
        Args:
            label_map (dict of str to str): A dictionary with keys consisting 
                of one or more of the following: 'State', 'Transition', and
                'Condition'. The values are the replacement values which the 
                user would like to use as node labels in the generated graph.
        """
        self.label_map = label_map

    @property
    def label_map(self):
        return self._label_map

    @label_map.setter
    def label_map(self, value):
        bad_labels = []
        try:
            for k in value.keys():
                if k not in ["State", "Transition", "Condition"]:
                    bad_labels.append(k)
            if bad_labels:
                raise ValueError(
                    "The only customisable labels are 'State', "
                    + "'Transition' and 'Condition'. The following are not  "
                    + "allowed: "
                    + str(bad_labels)
                )
            else:
                self._label_map = value
        except AttributeError:
            self._label_map = {}

    @property
    def state(self):
        try:
            return self.label_map["State"]
        except KeyError:
            return "State"

    @property
    def transition(self):
        try:
            return self.label_map["Transition"]
        except KeyError:
            return "Transition"

    @property
    def condition(self):
        try:
            return self.label_map["Condition"]
        except KeyError:
            return "Condition"
