{ "name1": "Sue", "name2": "Sarah" }

// statement 1
MERGE (n1:TestNode {role:"influencer", name:$name1})-[:INFLUENCES]->
    (n2:TestNode {name:"Billy", role:"follower"});
// sttatement 2
MATCH (n:TestNode {role:"influencer"})
MERGE (n)-[:INFLUENCES]->(:TestNode {role:"follower", name:$name2});