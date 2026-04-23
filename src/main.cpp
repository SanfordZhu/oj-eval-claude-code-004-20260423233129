#include "bookstore.h"
#include <iostream>
#include <string>

int main() {
    BookStore store;
    if (!store.init()) {
        return 1;
    }

    std::string line;
    while (std::getline(std::cin, line)) {
        if (!store.processCommand(line)) {
            break;
        }
    }

    return 0;
}
