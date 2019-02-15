{ "name1": "Sue" }

// statement 1
MERGE (n1:TestNode {role:"influencer", name:$name1})-[:INFLUENCES]->
    (n2:TestNode {name:"Billy", role:"follower"});
// statement 2
MATCH (n:TestNode {role:"influencer"})
MERGE (n)-[:INFLUENCES]->(:TestNode {role:"follower", name:$name2});