#include <iostream>

#include "kuzu.hpp"
using namespace kuzu::main;

int main() {
    auto database = std::make_unique<Database>("test" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());

    // Create schema.
//    auto res = connection->query("BEGIN TRANSACTION;");
//    std::cout << res->toString() << std::endl;
//    res = connection->query("CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY (name))");
//    std::cout << res->toString() << std::endl;
//    res = connection->query("CREATE REL TABLE Follows(FROM User TO User, since INT64)");
//    std::cout << res->toString() << std::endl;
//    res = connection->query("COPY User From \"/Users/monkey/Desktop/博士/code/KUZU_PROJECT/kuzu/dataset/demo-db/csv/user.csv\"");
//    std::cout << res->toString() << std::endl;
//    res = connection->query("COMMIT;");
//    std::cout << res->toString() << std::endl;
    auto res = connection->query("MATCH (u:User) WHERE u.name = 'Adam' RETURN u.age");
    std::cout << res->toString() << std::endl;
    // Create nodes.
//    connection->query("CREATE (:Person {name: 'Alice', age: 25});");
//    connection->query("CREATE (:Person {name: 'Bob', age: 30});");

    // Execute a simple query.
//    auto result = connection->query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;");
    // Print query result.
//    std::cout << result->toString();
}
