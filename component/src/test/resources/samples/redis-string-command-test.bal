import ballerina.data.redis;

function testSet() (string) {
    endpoint<redis:ClientConnector> conn {
        create redis:ClientConnector("localhost", "", {});
    }
    string result = conn.set("Name", "Manuri");
    string _ = conn.quit();
    return result;
}


