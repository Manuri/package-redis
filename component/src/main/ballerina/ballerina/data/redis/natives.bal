package ballerina.data.redis;

public struct ConnectionProperties {
    string codec;

    boolean poolingEnabled;
    boolean isClusterConnection;
}


public connector ClientConnector (string hosts, string password, ConnectionProperties options) {

    map sharedMap = {};

    native action append (string key, string value);

    native action bitcount (string key);

    native action get (string key) (string);

    native action set (string key, string value) (string);

    native action lpush(string key, string[] values) (int);

    native action lpop(string key) (string);

    native action quit() (string);
}