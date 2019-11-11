from __future__ import print_function

import os
import datetime

from future.utils import iteritems
import pandas as pd


class EnvironTransition(object):
    """Representation of a single environmental transition."""

    def __init__(self, start_state, end_state, time, env_conds):
        """
        Args:
            start_state (str): Start state for transition
            end_state (str): End state for transition
            time (str): Time taken for transition
            env_conds (dict): Key/value pairs of environmental conditions
                which lead to transition
        """
        self.start_state = start_state
        self.end_state = end_state
        self.time = time
        self.env_conds = env_conds

    def _key_value_string_repr(self, key, val):
        """Return a string describing a given env condition and its state.

        Punctuation should be appropriate given type, e.g.
        time:10 (integer)
        good:true (boolean)
        water:"xeric" (string)
        """
        if isinstance(val, str):
            return '{0}:"{1}"'.format(key, val)
        elif isinstance(val, bool):
            return "{0}:{1}".format(key, repr(val).lower())
        else:
            return "{0}:{1}".format(key, val)

    def time_as_string(self):
        return "delta_t:{0}".format(self.time)

    def env_cond_as_string(self):
        states = []
        for (cond, state) in iteritems(self.env_conds):
            states.append(self._key_value_string_repr(cond, state))
        return ",\n".join(states)

    def __repr__(self):
        return (
            self._key_value_string_repr("start", self.start_state)
            + ",\n"
            + self._key_value_string_repr("end", self.end_state)
            + ",\n"
            + self.env_cond_as_string()
            + ",\n"
            + self.time_as_string()
        )


class EnvironTransitionSet(object):
    """Representation of all possible environmental transitions."""

    def __init__(
        self, df, start_state_col, end_state_col, time_col, env_cond_cols=None
    ):
        """Setup EnvironTransitionSet object from table.

        Args:
            df (pd.DataFrame): Table specifying environmental transitions.
            start_state_col (str): Name of table column specifying transition
                start state.
            end_state_col (str): Name of table column specifying transition
                end state.
            time_col (str): Name of table column specifying time taken to
                complete the transition specified by each row.
            env_cond_cols (list of str, optional): Names of columns which
                specify combinations of environmental conditions leading
                to the transition specified by each row. If not given
                these will be assumed to be all the columns in the DataFrame
                not already specified.
        """
        processed_env_cond_cols = self._infer_env_cond_cols(
            df, start_state_col, end_state_col, time_col, env_cond_cols
        )

        self._transitions = self._process_environ_transitions(
            df, start_state_col, end_state_col, time_col, processed_env_cond_cols
        )

    def _infer_env_cond_cols(
        self, df, start_state_col, end_state_col, time_col, env_cond_cols
    ):
        """Work out which columns specify environmental conditions.

        If `env_cond_cols` specified in constructor, use those. Otherwise
        assume every column not already specified is an environmental
        condition.
        """
        if env_cond_cols:
            return env_cond_cols
        else:
            return [
                col
                for col in df.columns
                if col not in [start_state_col, end_state_col, time_col]
            ]

    def _process_environ_transitions(
        self, df, start_state_col, end_state_col, time_col, env_cond_cols
    ):
        """Process table, return list of EnvironCondition objects."""
        env_conds = []
        for index, row in df.iterrows():
            cond = row[env_cond_cols].to_dict()
            env_conds.append(
                EnvironTransition(
                    row[start_state_col], row[end_state_col], row[time_col], cond
                )
            )
        return env_conds

    @property
    def transitions(self):
        return self._transitions

    @transitions.setter
    def transitions(self, value):
        self._transitions = value

    def _make_dict_mapper(self, data_dict):
        def get_value(key):
            if key in data_dict.keys():
                return data_dict[key]
            else:
                return key

        return get_value

    def apply_state_aliases(self, state_aliases):
        """Consume state alias dict and apply to each EnvironTransition.

        Be careful to ensure the type of the keys in the state_alias dict
        match the type of the state codes included in the input data.
        """
        state_alias_mapper = self._make_dict_mapper(state_aliases)
        for et in self.transitions:
            et.start_state = state_alias_mapper(et.start_state)
            et.end_state = state_alias_mapper(et.end_state)

    def apply_environ_condition_aliases(self, env_cond_aliases):
        """Consume env condition aliases dict, apply to EnvironTransition-s."""
        alias_mappers = {}
        for k in env_cond_aliases.keys():
            # make a dictionary of functions which return value alias if given
            alias_mappers[k] = self._make_dict_mapper(env_cond_aliases[k])

        for et in self.transitions:
            for k in et.env_conds.keys():
                try:
                    et.env_conds[k] = alias_mappers[k](et.env_conds[k])
                except KeyError as e:
                    print(
                        "WARNING: couldn't find alias for environmental"
                        + " condition {0}".format(k)
                        + e
                    )

    def _get_header_str(self, project_path, start_code, end_code):
        """Construct the header portion of the Cypher file.

        This will specify the SuccessionTrajectory between ``start_code`` and
        ``end_code`` along with all the possible combinations of
        EnvironConditions which cause that transition.
        """
        header_str = """
//==============================================================================
        // file: {0}/{1}_to_{2}_w.cql
        // modified: {3}
        // dependencies:
        //     abstract/LandCoverType_w.cql
        // external parameters:
        //     model_ID, used to identify model created nodes belong to 
        // description:
        //     Create the Successiontrajectory representing the possibility of 
        //     transition between the {1} and {2} LandCoverState-s. Also specify all 
        //     combinations of environmental conditions which can lead to this 
        //     transition.
        //==============================================================================

        {{
         "priority":1
        }}
        """.format(
            project_path, start_code, end_code, str(datetime.date.today())
        )
        return header_str

    def _get_succession_traj_query(self, start_code, end_code):
        """Return a string containing a SuccessionTrajectory query.

        This will specify the possibility of transitioning from ``start_code``
        to ``end_code``
        """
        traj_query = """
        MATCH
          (srcLCT:LandCoverType {{code:\"{0}\", model_ID:$model_ID}}),
          (tgtLCT:LandCoverType {{code:\"{1}\", model_ID:$model_ID}})
        CREATE
          (traj:SuccessionTrajectory {{model_ID:$model_ID}})
        MERGE (srcLCT)<-[:SOURCE]-(traj)-[:TARGET]->(tgtLCT);

        """.format(
            start_code, end_code
        )
        return traj_query

    def _get_env_cond_query(self, env_trans):
        """Given an EnvironTransition construct environ conditions query."""
        env_cond_query = """
        MERGE
          (ec:EnvironCondition {{model_ID:$model_ID,
                                {0},
                                {1}}})
        WITH ec
        MATCH
          (:LandCoverType {{code:\"{2}\", model_ID:$model_ID}})
          <-[:SOURCE]-(traj:SuccessionTrajectory {{model_ID:$model_ID}})-[:TARGET]->
          (:LandCoverType {{code:\"{3}\", model_ID:$model_ID}})
        MERGE
          (ec)-[:CAUSES]->(traj);
        """.format(
            env_trans.env_cond_as_string().replace(",\n", ",\n" + 32 * " "),
            env_trans.time_as_string(),
            env_trans.start_state,
            env_trans.end_state,
        )
        return env_cond_query

    def _get_file_dict(self):
        """Return file name/ file contents key/value pairs."""
        d = {}
        for trans in self.transitions:
            start = trans.start_state
            end = trans.end_state
            fname = start + "_to_" + end + "_w.cql"
            if fname not in d:
                d[fname] = self._get_header_str(
                    "succession", start, end
                ) + self._get_succession_traj_query(start, end)
            d[fname] += self._get_env_cond_query(trans)

        return d

    def write_cypher_files(self, project_path):
        target_dir = os.path.join(project_path, "succession")
        if not os.path.isdir(target_dir):
            os.makedirs(target_dir)

        d = self._get_file_dict()
        for k in d.keys():
            fname = os.path.join(target_dir, k)
            with open(fname, "w") as f:
                f.write(d[k])
