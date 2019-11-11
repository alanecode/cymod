from __future__ import print_function

import os
import datetime
import unittest
from backports import tempfile

# numpy sanctioned hack to avoid irritating and unuseful warnings
# https://github.com/numpy/numpy/pull/432/commits/170ed4e33d6196d724dc18ddcd42311c291b4587?diff=split
import warnings

warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

import pandas as pd
from cymod.transtable import EnvironTransitionSet, EnvironTransition

global test_data_dir
test_data_dir = os.path.join("tests", "resources")


class EnvironTransitionTestCase(unittest.TestCase):
    def test_repr(self):
        envt = EnvironTransition(
            "state1", "state2", 10, {"good": False, "water": "hydric"}
        )

        test_str = (
            'start:"state1",\nend:"state2",\nwater:"hydric",\n'
            + "good:false,\ndelta_t:10"
        )

        self.assertEqual(repr(envt), test_str)


class ExplicitTestTableTestCase(unittest.TestCase):
    """Tests using a table whose data are stated explicitly.

    That is, as opposed to being aliased.
    """

    def get_explicit_test_table(self):
        """Read test data with explicit values and return DataFrame."""
        test_data_file = os.path.join(test_data_dir, "test_table_explicit_data.csv")
        return pd.read_csv(test_data_file)

    def setUp(self):
        self.df = self.get_explicit_test_table()
        self.env_trans_set = EnvironTransitionSet(self.df, "start", "end", "deltat")

    def tearDown(self):
        self.env_trans_set = None

    def test_load_two_transitions(self):
        self.assertEqual(len(self.env_trans_set.transitions), 2)

    def test_env_cond_query(self):
        test_envt = EnvironTransition(
            "state1", "state2", 10, {"good": False, "water": "hydric"}
        )

        correct_query = """
        MERGE
          (ec:EnvironCondition {model_ID:$model_ID,
                                water:"hydric",
                                good:false,
                                delta_t:10})
        WITH ec
        MATCH
          (:LandCoverType {code:"state1", model_ID:$model_ID})
          <-[:SOURCE]-(traj:SuccessionTrajectory {model_ID:$model_ID})-[:TARGET]->
          (:LandCoverType {code:"state2", model_ID:$model_ID})
        MERGE
          (ec)-[:CAUSES]->(traj);
        """

        self.assertEqual(
            self.env_trans_set._get_env_cond_query(test_envt), correct_query
        )

    def test_two_files_written(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            self.env_trans_set.write_cypher_files(tmpdir)
            target_dir = os.path.join(tmpdir, "succession")
            self.assertTrue(len(os.listdir(target_dir)), 2)

    def test_cypher_file_contents_correct(self):

        correct_file_contents = """
//==============================================================================
        // file: succession/state1_to_state2_w.cql
        // modified: {0}
        // dependencies:
        //     abstract/LandCoverType_w.cql
        // external parameters:
        //     model_ID, used to identify model created nodes belong to 
        // description:
        //     Create the Successiontrajectory representing the possibility of 
        //     transition between the state1 and state2 LandCoverState-s. Also specify all 
        //     combinations of environmental conditions which can lead to this 
        //     transition.
        //==============================================================================

        {{
         "priority":1
        }}
        
        MATCH
          (srcLCT:LandCoverType {{code:"state1", model_ID:$model_ID}}),
          (tgtLCT:LandCoverType {{code:"state2", model_ID:$model_ID}})
        CREATE
          (traj:SuccessionTrajectory {{model_ID:$model_ID}})
        MERGE (srcLCT)<-[:SOURCE]-(traj)-[:TARGET]->(tgtLCT);

        
        MERGE
          (ec:EnvironCondition {{model_ID:$model_ID,
                                cond1:false,
                                cond2:"low",
                                delta_t:2}})
        WITH ec
        MATCH
          (:LandCoverType {{code:"state1", model_ID:$model_ID}})
          <-[:SOURCE]-(traj:SuccessionTrajectory {{model_ID:$model_ID}})-[:TARGET]->
          (:LandCoverType {{code:"state2", model_ID:$model_ID}})
        MERGE
          (ec)-[:CAUSES]->(traj);
        """.format(
            str(datetime.date.today())
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            self.env_trans_set.write_cypher_files(tmpdir)
            test_file = os.path.join(tmpdir, "succession", "state1_to_state2_w.cql")
            with open(test_file, "rb") as f:
                self.assertEqual(f.read(), correct_file_contents)


class AliasedTestTableTestCase(unittest.TestCase):
    """Test case for a table with coded data needing aliases."""

    def get_coded_test_table(self):
        """Read test data with coded values needing aliases."""
        test_data_file = os.path.join(test_data_dir, "test_table_coded_data.csv")

        return pd.read_csv(test_data_file)

    def setUp(self):
        self.df = self.get_coded_test_table()
        self.env_trans_set = EnvironTransitionSet(self.df, "start", "end", "deltat")

        self.state_aliases = {0: "state1", 1: "state2", 2: "state3"}

        self.env_cond_aliases = {
            "cond1": {0: False, 1: True},
            "cond2": {0: "low", 1: "medium", 2: "high"},
        }

    def tearDown(self):
        self.env_trans_set = None
        self.state_aliases = None
        self.env_cond_aliases = None

    def test_apply_state_aliases(self):
        self.env_trans_set.apply_state_aliases(self.state_aliases)

        et0 = self.env_trans_set.transitions[0]
        self.assertEqual(et0.start_state, "state1")
        self.assertEqual(et0.end_state, "state2")

        et1 = self.env_trans_set.transitions[1]
        self.assertEqual(et1.start_state, "state2")
        self.assertEqual(et1.end_state, "state3")

    def test_env_cond_aliases(self):
        self.env_trans_set.apply_environ_condition_aliases(self.env_cond_aliases)

        et0 = self.env_trans_set.transitions[0]
        et0.env_conds["cond1"]
        self.assertEqual(et0.env_conds["cond1"], False)
        self.assertEqual(et0.env_conds["cond2"], "low")

        et1 = self.env_trans_set.transitions[1]
        self.assertEqual(et1.env_conds["cond1"], True)
        self.assertEqual(et1.env_conds["cond2"], "high")

    def test_cypher_file_contents_correct(self):

        correct_file_contents = """
//==============================================================================
        // file: succession/state1_to_state2_w.cql
        // modified: {0}
        // dependencies:
        //     abstract/LandCoverType_w.cql
        // external parameters:
        //     model_ID, used to identify model created nodes belong to 
        // description:
        //     Create the Successiontrajectory representing the possibility of 
        //     transition between the state1 and state2 LandCoverState-s. Also specify all 
        //     combinations of environmental conditions which can lead to this 
        //     transition.
        //==============================================================================

        {{
         "priority":1
        }}
        
        MATCH
          (srcLCT:LandCoverType {{code:"state1", model_ID:$model_ID}}),
          (tgtLCT:LandCoverType {{code:"state2", model_ID:$model_ID}})
        CREATE
          (traj:SuccessionTrajectory {{model_ID:$model_ID}})
        MERGE (srcLCT)<-[:SOURCE]-(traj)-[:TARGET]->(tgtLCT);

        
        MERGE
          (ec:EnvironCondition {{model_ID:$model_ID,
                                cond1:false,
                                cond2:"low",
                                delta_t:2}})
        WITH ec
        MATCH
          (:LandCoverType {{code:"state1", model_ID:$model_ID}})
          <-[:SOURCE]-(traj:SuccessionTrajectory {{model_ID:$model_ID}})-[:TARGET]->
          (:LandCoverType {{code:"state2", model_ID:$model_ID}})
        MERGE
          (ec)-[:CAUSES]->(traj);
        """.format(
            str(datetime.date.today())
        )

        # apply aliases
        self.env_trans_set.apply_state_aliases(self.state_aliases)
        self.env_trans_set.apply_environ_condition_aliases(self.env_cond_aliases)

        with tempfile.TemporaryDirectory() as tmpdir:
            self.env_trans_set.write_cypher_files(tmpdir)
            test_file = os.path.join(tmpdir, "succession", "state1_to_state2_w.cql")
            with open(test_file, "rb") as f:
                self.assertEqual(f.read(), correct_file_contents)


if __name__ == "__main__":
    unittest.main()
