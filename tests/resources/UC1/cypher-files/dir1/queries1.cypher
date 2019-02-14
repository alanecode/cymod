// Statement 1
MERGE (n1:TestNode {role:"influencer", name:"Sue"})-[:INFLUENCES]->
    (n2:TestNode {name:"Billy", role:"follower"});
                
// Statement 2
MATCH (n:TestNode {role:"influencer"})
MERGE (n)-[:INFLUENCES]->(:TestNode {role:"follower", name:"Sarah"});

// Statement 3
MERGE (n:TestNode {role:"loner", name:"Tim"});