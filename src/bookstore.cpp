#include "bookstore.h"
#include <algorithm>
#include <cctype>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <sys/stat.h>

#ifdef _WIN32
#include <direct.h>
#define mkdir(dir, mode) _mkdir(dir)
#endif

BookStore::BookStore(const std::string& dir)
    : dataDir(dir), accountsFile(dir + "/accounts.dat"),
      booksFile(dir + "/books.dat"), transactionsFile(dir + "/transactions.dat"),
      counterFile(dir + "/counter.dat"), nextTransactionId(1), initialized(false) {
}

BookStore::~BookStore() {
    closeFiles();
}

bool BookStore::init() {
    // Create data directory if it doesn't exist
    struct stat st = {0};
    if (stat(dataDir.c_str(), &st) == -1) {
        mkdir(dataDir.c_str(), 0755);
    }

    if (!openFiles()) {
        if (!createDefaultFiles()) {
            return false;
        }
        if (!openFiles()) {
            return false;
        }
    }

    // Rebuild indexes
    rebuildAccountIndex();
    rebuildBookIndex();

    nextTransactionId = readNextTransactionId();
    initialized = true;
    return true;
}

bool BookStore::openFiles() {
    accountsStream.open(accountsFile, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!accountsStream.is_open()) return false;

    booksStream.open(booksFile, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!booksStream.is_open()) return false;

    transactionsStream.open(transactionsFile, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!transactionsStream.is_open()) return false;

    return true;
}

void BookStore::closeFiles() {
    if (accountsStream.is_open()) accountsStream.close();
    if (booksStream.is_open()) booksStream.close();
    if (transactionsStream.is_open()) transactionsStream.close();
}

bool BookStore::createDefaultFiles() {
    // Create empty files
    {
        std::ofstream acc(accountsFile, std::ios::binary);
        if (!acc.is_open()) return false;
        // Write default root account
        // Format: userid_size|userid|pwd_size|pwd|uname_size|uname|privilege\n
    }
    {
        std::ofstream books(booksFile, std::ios::binary);
        if (!books.is_open()) return false;
    }
    {
        std::ofstream trans(transactionsFile, std::ios::binary);
        if (!trans.is_open()) return false;
    }
    {
        std::ofstream cnt(counterFile, std::ios::binary);
        if (!cnt.is_open()) return false;
        cnt.write("1", 1);
    }

    // Add default root account
    Account root("root", "sjtu", "root", 7);
    if (!addAccount(root)) {
        return false;
    }

    return true;
}

int BookStore::readNextTransactionId() {
    std::ifstream in(counterFile);
    int id = 1;
    if (in.is_open()) {
        in >> id;
        in.close();
    }
    return id > 0 ? id : 1;
}

void BookStore::writeNextTransactionId(int id) {
    std::ofstream out(counterFile, std::ios::trunc);
    if (out.is_open()) {
        out << id;
        out.close();
    }
    nextTransactionId = id;
}

void BookStore::rebuildAccountIndex() {
    std::lock_guard<std::mutex> lock(fileMutex);
    accountIndex.clear();
    accountsStream.clear();
    accountsStream.seekg(0);

    std::string line;
    while (std::getline(accountsStream, line)) {
        if (line.empty()) continue;

        std::streamoff offset = accountsStream.tellg();
        offset -= (line.size() + 1); // subtract line length + newline

        size_t firstBar = line.find('|');
        if (firstBar == std::string::npos) continue;

        int uidSize = std::stoi(line.substr(0, firstBar));
        size_t pos = firstBar + 1;
        std::string uid = line.substr(pos, uidSize);

        accountIndex[uid] = {offset, (int)line.size() + 1}; // include newline
    }
}

void BookStore::rebuildBookIndex() {
    std::lock_guard<std::mutex> lock(fileMutex);
    bookIndex.clear();
    booksStream.clear();
    booksStream.seekg(0);

    std::string line;
    while (std::getline(booksStream, line)) {
        if (line.empty()) continue;

        std::streamoff offset = booksStream.tellg();
        offset -= (line.size() + 1); // subtract line length + newline

        size_t firstBar = line.find('|');
        if (firstBar == std::string::npos) continue;

        int isbnSize = std::stoi(line.substr(0, firstBar));
        size_t pos = firstBar + 1;
        std::string isbn = line.substr(pos, isbnSize);

        bookIndex[isbn] = {offset, (int)line.size() + 1}; // include newline
    }
}

// Account operations
bool BookStore::findAccount(const std::string& userid, Account& out) {
    std::lock_guard<std::mutex> lock(fileMutex);

    auto it = accountIndex.find(userid);
    if (it == accountIndex.end()) {
        return false;
    }

    accountsStream.clear();
    accountsStream.seekg(it->second.offset);

    std::string line;
    std::getline(accountsStream, line);
    if (line.empty()) return false;

    std::istringstream iss(line);
    int uidSize, pwdSize, unameSize, priv;
    if (!(iss >> uidSize)) return false;

    char delim;
    iss >> delim;
    if (delim != '|') return false;

    std::string uid;
    uid.resize(uidSize);
    iss.read(&uid[0], uidSize);
    iss >> delim;
    if (delim != '|') return false;

    iss >> pwdSize >> delim;
    std::string pwd;
    pwd.resize(pwdSize);
    iss.read(&pwd[0], pwdSize);
    iss >> delim;
    if (delim != '|') return false;

    iss >> unameSize >> delim;
    std::string uname;
    uname.resize(unameSize);
    iss.read(&uname[0], unameSize);
    iss >> delim;
    if (delim != '|') return false;

    iss >> priv;

    out.userid = uid;
    out.password = pwd;
    out.username = uname;
    out.privilege = priv;
    return true;
}

bool BookStore::accountExists(const std::string& userid) {
    return accountIndex.count(userid) > 0;
}

bool BookStore::addAccount(const Account& account) {
    std::lock_guard<std::mutex> lock(fileMutex);
    if (accountExists(account.userid)) {
        return false;
    }

    accountsStream.clear();
    accountsStream.seekp(0, std::ios::end);
    std::streamoff offset = accountsStream.tellp();

    // Format: uidSize|uid|pwdSize|pwd|unameSize|uname|priv\n
    accountsStream << account.userid.size() << "|"
                  << account.userid << "|"
                  << account.password.size() << "|"
                  << account.password << "|"
                  << account.username.size() << "|"
                  << account.username << "|"
                  << account.privilege << "\n";

    if (accountsStream.good()) {
        std::streamoff endOffset = accountsStream.tellp();
        int lineLen = (int)(endOffset - offset);
        accountIndex[account.userid] = {offset, lineLen};
        return true;
    }
    return false;
}

bool BookStore::updateAccount(const Account& account) {
    std::lock_guard<std::mutex> lock(fileMutex);
    if (!accountIndex.count(account.userid)) return false;

    std::vector<std::string> lines;
    accountsStream.clear();
    accountsStream.seekg(0);

    std::string line;
    bool found = false;
    while (std::getline(accountsStream, line)) {
        if (line.empty()) continue;

        size_t firstBar = line.find('|');
        if (firstBar == std::string::npos) {
            lines.push_back(line);
            continue;
        }

        int uidSize = std::stoi(line.substr(0, firstBar));
        size_t pos = firstBar + 1;
        std::string uid = line.substr(pos, uidSize);

        if (uid == account.userid) {
            // Rebuild the line
            std::ostringstream oss;
            oss << account.userid.size() << "|"
                << account.userid << "|"
                << account.password.size() << "|"
                << account.password << "|"
                << account.username.size() << "|"
                << account.username << "|"
                << account.privilege << "\n";
            lines.push_back(oss.str());
            found = true;
        } else {
            lines.push_back(line + "\n");
        }
    }

    if (!found) return false;

    accountIndex.clear();
    accountsStream.close();
    std::ofstream out(accountsFile, std::ios::trunc | std::ios::binary);
    std::streamoff offset = 0;
    for (const auto& l : lines) {
        out << l;
        std::string::size_type lineLen = l.size();
        // Find the userid in this line for index
        size_t firstBar = l.find('|');
        if (firstBar != std::string::npos) {
            int uidSize = std::stoi(l.substr(0, firstBar));
            size_t pos = firstBar + 1;
            std::string uid = l.substr(pos, uidSize);
            accountIndex[uid] = {offset, (int)lineLen};
        }
        offset += lineLen;
    }
    out.flush();
    out.close();

    accountsStream.open(accountsFile, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);

    return accountsStream.is_open();
}

bool BookStore::deleteAccount(const std::string& userid) {
    std::lock_guard<std::mutex> lock(fileMutex);
    if (!accountIndex.count(userid)) return false;

    std::vector<std::string> lines;
    accountsStream.clear();
    accountsStream.seekg(0);

    std::string line;
    bool found = false;
    while (std::getline(accountsStream, line)) {
        if (line.empty()) continue;

        size_t firstBar = line.find('|');
        if (firstBar == std::string::npos) continue;

        int uidSize = std::stoi(line.substr(0, firstBar));
        size_t pos = firstBar + 1;
        std::string uid = line.substr(pos, uidSize);

        if (uid != userid) {
            lines.push_back(line + "\n");
        } else {
            found = true;
        }
    }

    if (!found) return false;

    accountIndex.clear();
    accountsStream.close();
    std::ofstream out(accountsFile, std::ios::trunc | std::ios::binary);
    std::streamoff offset = 0;
    for (const auto& l : lines) {
        out << l;
        std::string::size_type lineLen = l.size();
        // Find the userid in this line for index
        size_t firstBar = l.find('|');
        if (firstBar != std::string::npos) {
            int uidSize = std::stoi(l.substr(0, firstBar));
            size_t pos = firstBar + 1;
            std::string uid = l.substr(pos, uidSize);
            accountIndex[uid] = {offset, (int)lineLen};
        }
        offset += lineLen;
    }
    out.flush();
    out.close();

    accountsStream.open(accountsFile, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);

    return accountsStream.is_open();
}

bool BookStore::isUserLoggedIn(const std::string& userid) {
    std::stack<Account> temp = loginStack;
    while (!temp.empty()) {
        if (temp.top().userid == userid) {
            return true;
        }
        temp.pop();
    }
    return false;
}

// Book operations
bool BookStore::findBook(const std::string& isbn, Book& out) {
    std::lock_guard<std::mutex> lock(fileMutex);

    auto it = bookIndex.find(isbn);
    if (it == bookIndex.end()) {
        return false;
    }

    booksStream.clear();
    booksStream.seekg(it->second.offset);

    std::string line;
    std::getline(booksStream, line);
    if (line.empty()) return false;

    std::istringstream iss(line);
    Book book;
    int isbnSize, nameSize, authorSize, keywordSize;
    char delim;

    if (!(iss >> isbnSize)) return false;
    iss >> delim;
    if (delim != '|') return false;

    book.isbn.resize(isbnSize);
    iss.read(&book.isbn[0], isbnSize);
    iss >> delim;
    if (delim != '|') return false;

    iss >> nameSize >> delim;
    book.name.resize(nameSize);
    iss.read(&book.name[0], nameSize);
    iss >> delim;
    if (delim != '|') return false;

    iss >> authorSize >> delim;
    book.author.resize(authorSize);
    iss.read(&book.author[0], authorSize);
    iss >> delim;
    if (delim != '|') return false;

    iss >> keywordSize >> delim;
    book.keyword.resize(keywordSize);
    iss.read(&book.keyword[0], keywordSize);
    iss >> delim;
    if (delim != '|') return false;

    iss >> book.price >> delim >> book.stock;

    out = book;
    return true;
}

bool BookStore::bookExists(const std::string& isbn) {
    return bookIndex.count(isbn) > 0;
}

bool BookStore::addBook(const Book& book) {
    std::lock_guard<std::mutex> lock(fileMutex);
    if (bookExists(book.isbn)) {
        return false;
    }

    booksStream.clear();
    booksStream.seekp(0, std::ios::end);
    std::streamoff offset = booksStream.tellp();

    booksStream << book.isbn.size() << "|"
                << book.isbn << "|"
                << book.name.size() << "|"
                << book.name << "|"
                << book.author.size() << "|"
                << book.author << "|"
                << book.keyword.size() << "|"
                << book.keyword << "|"
                << std::fixed << std::setprecision(2) << book.price << "|"
                << book.stock << "\n";

    if (booksStream.good()) {
        std::streamoff endOffset = booksStream.tellp();
        int lineLen = (int)(endOffset - offset);
        bookIndex[book.isbn] = {offset, lineLen};
        return true;
    }
    return false;
}

bool BookStore::updateBook(const Book& book) {
    std::lock_guard<std::mutex> lock(fileMutex);
    if (!bookIndex.count(book.isbn)) return false;

    std::vector<std::string> lines;
    booksStream.clear();
    booksStream.seekg(0);

    std::string line;
    bool found = false;
    while (std::getline(booksStream, line)) {
        if (line.empty()) continue;

        size_t firstBar = line.find('|');
        if (firstBar == std::string::npos) {
            lines.push_back(line);
            continue;
        }

        int isbnSize = std::stoi(line.substr(0, firstBar));
        size_t pos = firstBar + 1;
        std::string isbn = line.substr(pos, isbnSize);

        if (isbn == book.isbn) {
            std::ostringstream oss;
            oss << book.isbn.size() << "|"
                << book.isbn << "|"
                << book.name.size() << "|"
                << book.name << "|"
                << book.author.size() << "|"
                << book.author << "|"
                << book.keyword.size() << "|"
                << book.keyword << "|"
                << std::fixed << std::setprecision(2) << book.price << "|"
                << book.stock << "\n";
            lines.push_back(oss.str());
            found = true;
        } else {
            lines.push_back(line + "\n");
        }
    }

    if (!found) return false;

    bookIndex.clear();
    booksStream.close();
    std::ofstream out(booksFile, std::ios::trunc | std::ios::binary);
    std::streamoff offset = 0;
    for (const auto& l : lines) {
        out << l;
        std::string::size_type lineLen = l.size();
        // Find the isbn in this line for index
        size_t firstBar = l.find('|');
        if (firstBar != std::string::npos) {
            int isbnSize = std::stoi(l.substr(0, firstBar));
            size_t pos = firstBar + 1;
            std::string isbn = l.substr(pos, isbnSize);
            bookIndex[isbn] = {offset, (int)lineLen};
        }
        offset += lineLen;
    }
    out.flush();
    out.close();

    booksStream.open(booksFile, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);

    return booksStream.is_open();
}

std::vector<Book> BookStore::searchBooksByISBN(const std::string& isbn) {
    std::vector<Book> result;
    Book book;
    if (findBook(isbn, book)) {
        result.push_back(book);
    }
    return result;
}

std::vector<Book> BookStore::getAllBooks() {
    std::vector<Book> result;
    std::lock_guard<std::mutex> lock(fileMutex);
    booksStream.clear();
    booksStream.seekg(0);

    std::string line;
    while (std::getline(booksStream, line)) {
        if (line.empty()) continue;
        std::istringstream iss(line);
        Book book;
        int isbnSize, nameSize, authorSize, keywordSize;
        char delim;

        if (!(iss >> isbnSize)) continue;
        iss >> delim;
        if (delim != '|') continue;

        book.isbn.resize(isbnSize);
        iss.read(&book.isbn[0], isbnSize);
        iss >> delim;
        if (delim != '|') continue;

        iss >> nameSize >> delim;
        book.name.resize(nameSize);
        iss.read(&book.name[0], nameSize);
        iss >> delim;
        if (delim != '|') continue;

        iss >> authorSize >> delim;
        book.author.resize(authorSize);
        iss.read(&book.author[0], authorSize);
        iss >> delim;
        if (delim != '|') continue;

        iss >> keywordSize >> delim;
        book.keyword.resize(keywordSize);
        iss.read(&book.keyword[0], keywordSize);
        iss >> delim;
        if (delim != '|') continue;

        iss >> book.price >> delim >> book.stock;
        result.push_back(book);
    }

    return result;
}

std::vector<Book> BookStore::searchBooksByName(const std::string& name) {
    std::vector<Book> result;
    auto all = getAllBooks();
    for (const auto& b : all) {
        if (b.name.find(name) != std::string::npos) {
            result.push_back(b);
        }
    }
    return result;
}

std::vector<Book> BookStore::searchBooksByAuthor(const std::string& author) {
    std::vector<Book> result;
    auto all = getAllBooks();
    for (const auto& b : all) {
        if (b.author.find(author) != std::string::npos) {
            result.push_back(b);
        }
    }
    return result;
}

std::vector<Book> BookStore::searchBooksByKeyword(const std::string& keyword) {
    std::vector<Book> result;
    auto all = getAllBooks();
    for (const auto& b : all) {
        // Check if keyword exists in the keyword list
        std::string kw = b.keyword;
        if (kw.empty()) continue;

        // Split by | and check if any matches
        size_t pos = 0;
        bool found = false;
        while (pos < kw.size() && !found) {
            size_t next = kw.find('|', pos);
            std::string token;
            if (next == std::string::npos) {
                token = kw.substr(pos);
            } else {
                token = kw.substr(pos, next - pos);
            }
            if (token == keyword) {
                found = true;
            }
            pos = (next == std::string::npos) ? kw.size() : next + 1;
        }
        if (found) {
            result.push_back(b);
        }
    }
    return result;
}

// Transaction operations
bool BookStore::addTransaction(const Transaction& trans) {
    std::lock_guard<std::mutex> lock(fileMutex);
    transactionsStream.clear();
    transactionsStream.seekp(0, std::ios::end);

    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    long long timestamp = std::chrono::duration_cast<std::chrono::seconds>(duration).count();

    Transaction t = trans;
    t.id = nextTransactionId;
    t.timestamp = timestamp;

    // Format: id|username|operation|isbn|quantity|amount|timestamp\n
    transactionsStream << t.id << "|"
                       << t.username << "|"
                       << t.operation << "|"
                       << t.isbn << "|"
                       << t.quantity << "|"
                       << std::fixed << std::setprecision(2) << t.amount << "|"
                       << t.timestamp << "\n";

    writeNextTransactionId(nextTransactionId + 1);
    return transactionsStream.good();
}

std::vector<Transaction> BookStore::getAllTransactions() {
    std::vector<Transaction> result;
    std::lock_guard<std::mutex> lock(fileMutex);
    transactionsStream.clear();
    transactionsStream.seekg(0);

    std::string line;
    while (std::getline(transactionsStream, line)) {
        if (line.empty()) continue;
        std::istringstream iss(line);
        Transaction trans;
        char delim;

        if (!(iss >> trans.id)) continue;
        iss >> delim;
        if (delim != '|') continue;

        std::getline(iss, trans.username, '|');
        std::getline(iss, trans.operation, '|');
        std::getline(iss, trans.isbn, '|');

        iss >> trans.quantity >> delim >> trans.amount >> delim >> trans.timestamp;
        result.push_back(trans);
    }

    return result;
}

std::vector<Transaction> BookStore::getLastTransactions(int count) {
    auto all = getAllTransactions();
    if (count >= all.size()) {
        return all;
    }
    return std::vector<Transaction>(all.end() - count, all.end());
}

int BookStore::getTransactionCount() {
    return (int)getAllTransactions().size();
}

// Validation helpers
bool BookStore::checkPrivilege(int required) const {
    return getCurrentPrivilege() >= required;
}

Account BookStore::getCurrentAccount() const {
    if (loginStack.empty()) {
        return Account();
    }
    return loginStack.top();
}

int BookStore::getCurrentPrivilege() const {
    if (loginStack.empty()) {
        return 0;
    }
    return loginStack.top().privilege;
}

bool BookStore::isValidUserId(const std::string& s) const {
    if (s.empty() || s.size() > 30) return false;
    for (char c : s) {
        if (!std::isalnum(c) && c != '_') return false;
    }
    return true;
}

bool BookStore::isValidPassword(const std::string& s) const {
    if (s.empty() || s.size() > 30) return false;
    for (char c : s) {
        if (!std::isalnum(c) && c != '_') return false;
    }
    return true;
}

bool BookStore::isValidPrivilege(const std::string& s) const {
    if (s.size() != 1) return false;
    if (!std::isdigit(s[0])) return false;
    int p = s[0] - '0';
    return p == 1 || p == 3 || p == 7;
}

bool BookStore::isValidUsername(const std::string& s) const {
    if (s.size() > 30) return false;
    for (char c : s) {
        if (std::isspace(c)) return false;
    }
    return true;
}

bool BookStore::isValidISBN(const std::string& s) const {
    if (s.empty() || s.size() > 20) return false;
    for (char c : s) {
        if (std::isspace(c)) return false;
    }
    return true;
}

bool BookStore::isValidBookName(const std::string& s) const {
    if (s.size() > 60) return false;
    for (char c : s) {
        if (std::isspace(c)) continue;
        if (c == '"') return false;
    }
    return true;
}

bool BookStore::isValidAuthor(const std::string& s) const {
    return isValidBookName(s);
}

bool BookStore::isValidKeyword(const std::string& s) const {
    if (s.size() > 60) return false;
    for (char c : s) {
        if (std::isspace(c)) continue;
        if (c == '"') return false;
    }
    return true;
}

bool BookStore::hasDuplicateKeywords(const std::string& keyword) const {
    std::vector<std::string> tokens;
    size_t pos = 0;
    while (pos < keyword.size()) {
        size_t next = keyword.find('|', pos);
        std::string token;
        if (next == std::string::npos) {
            token = keyword.substr(pos);
        } else {
            token = keyword.substr(pos, next - pos);
        }
        // Check for empty token
        if (token.empty()) return true;
        // Check duplicate
        for (const auto& t : tokens) {
            if (t == token) return true;
        }
        tokens.push_back(token);
        pos = (next == std::string::npos) ? keyword.size() : next + 1;
    }
    return false;
}

bool BookStore::isValidQuantity(const std::string& s) const {
    if (s.empty() || s.size() > 10) return false;
    for (char c : s) {
        if (!std::isdigit(c)) return false;
    }
    long long q = std::stoll(s);
    return q > 0 && q <= 2147483647LL;
}

bool BookStore::isValidPrice(const std::string& s) const {
    if (s.empty() || s.size() > 13) return false;
    bool hasDot = false;
    for (char c : s) {
        if (c == '.') {
            if (hasDot) return false;
            hasDot = true;
        } else if (!std::isdigit(c)) {
            return false;
        }
    }
    return true;
}

bool BookStore::isValidCount(const std::string& s) const {
    return isValidQuantity(s);
}

// Parsing helpers
std::vector<std::string> BookStore::splitLine(const std::string& line) const {
    std::vector<std::string> tokens;
    std::string current;
    bool inQuote = false;

    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        if (std::isspace(c)) {
            if (inQuote) {
                current += c;
            } else if (!current.empty()) {
                tokens.push_back(current);
                current.clear();
            }
        } else if (c == '"') {
            if (inQuote) {
                // End of quote
                inQuote = false;
                if (!current.empty()) {
                    tokens.push_back(current);
                    current.clear();
                }
            } else {
                inQuote = true;
            }
        } else {
            current += c;
        }
    }
    if (!current.empty()) {
        tokens.push_back(current);
    }
    return tokens;
}

bool BookStore::parseModifyArg(const std::string& arg, std::string& type, std::string& value) const {
    if (arg.size() < 5) return false;  // at least -x=y
    if (arg[0] != '-') return false;

    size_t eqPos = arg.find('=');
    if (eqPos == std::string::npos) return false;

    type = arg.substr(1, eqPos - 1);
    value = arg.substr(eqPos + 1);

    // Check for empty value
    if (value.empty()) return false;

    return true;
}

// Command handlers
bool BookStore::processCommand(const std::string& line) {
    // Check for empty line or all whitespace
    bool allSpace = true;
    for (char c : line) {
        if (!std::isspace(c)) {
            allSpace = false;
            break;
        }
    }
    if (allSpace) {
        return true;
    }

    std::vector<std::string> tokens = splitLine(line);
    if (tokens.empty()) {
        return true;
    }

    std::string cmd = tokens[0];

    if (cmd == "quit" || cmd == "exit") {
        return handleQuit();
    } else if (cmd == "su") {
        if (!handleSu(std::vector<std::string>(tokens.begin() + 1, tokens.end()))) {
            std::cout << "Invalid\n";
        }
    } else if (cmd == "logout") {
        if (!handleLogout()) {
            std::cout << "Invalid\n";
        }
    } else if (cmd == "register") {
        if (!handleRegister(std::vector<std::string>(tokens.begin() + 1, tokens.end()))) {
            std::cout << "Invalid\n";
        }
    } else if (cmd == "passwd") {
        if (!handlePasswd(std::vector<std::string>(tokens.begin() + 1, tokens.end()))) {
            std::cout << "Invalid\n";
        }
    } else if (cmd == "useradd") {
        if (!handleUseradd(std::vector<std::string>(tokens.begin() + 1, tokens.end()))) {
            std::cout << "Invalid\n";
        }
    } else if (cmd == "delete") {
        if (!handleDelete(std::vector<std::string>(tokens.begin() + 1, tokens.end()))) {
            std::cout << "Invalid\n";
        }
    } else if (cmd == "show") {
        if (!handleShow(std::vector<std::string>(tokens.begin() + 1, tokens.end()))) {
            std::cout << "Invalid\n";
        }
    } else if (cmd == "buy") {
        if (!handleBuy(std::vector<std::string>(tokens.begin() + 1, tokens.end()))) {
            std::cout << "Invalid\n";
        }
    } else if (cmd == "select") {
        if (!handleSelect(std::vector<std::string>(tokens.begin() + 1, tokens.end()))) {
            std::cout << "Invalid\n";
        }
    } else if (cmd == "modify") {
        if (!handleModify(std::vector<std::string>(tokens.begin() + 1, tokens.end()))) {
            std::cout << "Invalid\n";
        }
    } else if (cmd == "import") {
        if (!handleImport(std::vector<std::string>(tokens.begin() + 1, tokens.end()))) {
            std::cout << "Invalid\n";
        }
    } else if (cmd == "log") {
        if (!handleLog()) {
            std::cout << "Invalid\n";
        }
    } else if (cmd == "report" && tokens.size() >= 2) {
        std::string subcmd = tokens[1];
        if (subcmd == "finance" && !handleReportFinance()) {
            std::cout << "Invalid\n";
        } else if (subcmd == "employee" && !handleReportEmployee()) {
            std::cout << "Invalid\n";
        } else {
            std::cout << "Invalid\n";
        }
    } else {
        std::cout << "Invalid\n";
    }

    return true;
}

bool BookStore::handleSu(const std::vector<std::string>& args) {
    // su [UserID] ([Password])?
    if (args.empty() || args.size() > 2) return false;
    std::string userid = args[0];
    if (!isValidUserId(userid)) return false;

    Account acc;
    if (!findAccount(userid, acc)) return false;

    int currentPriv = getCurrentPrivilege();
    if (args.size() == 1) {
        // No password provided
        if (currentPriv < acc.privilege) {
            // Current privilege is lower, need password
            return false;
        }
        // Allow login
    } else {
        // Password provided
        std::string password = args[1];
        if (!isValidPassword(password)) return false;
        if (acc.password != password) return false;
    }

    loginStack.push(acc);
    return true;
}

bool BookStore::handleLogout() {
    // logout - requires privilege >= 1
    if (!checkPrivilege(1)) return false;
    if (loginStack.empty()) return false;
    loginStack.pop();
    return true;
}

bool BookStore::handleRegister(const std::vector<std::string>& args) {
    // register [UserID] [Password] [Username] - privilege 0 required
    if (args.size() != 3) return false;
    std::string userid = args[0];
    std::string password = args[1];
    std::string username = args[2];

    if (!isValidUserId(userid)) return false;
    if (!isValidPassword(password)) return false;
    if (!isValidUsername(username)) return false;

    if (accountExists(userid)) return false;

    Account acc(userid, password, username, 1);
    return addAccount(acc);
}

bool BookStore::handlePasswd(const std::vector<std::string>& args) {
    // passwd [UserID] ([CurrentPassword])? [NewPassword] - privilege >= 1
    if (!checkPrivilege(1)) return false;
    if (args.size() < 2 || args.size() > 3) return false;

    std::string userid = args[0];
    std::string newPassword;
    std::string currentPassword;

    if (args.size() == 2) {
        // Current password omitted - must be root
        if (getCurrentPrivilege() != 7) return false;
        newPassword = args[1];
    } else {
        currentPassword = args[1];
        newPassword = args[2];
    }

    if (!isValidUserId(userid)) return false;
    if (!isValidPassword(newPassword)) return false;
    if (!currentPassword.empty() && !isValidPassword(currentPassword)) return false;

    Account acc;
    if (!findAccount(userid, acc)) return false;

    if (getCurrentPrivilege() != 7) {
        // Not root, must check current password
        if (acc.password != currentPassword) return false;
    }

    acc.password = newPassword;
    return updateAccount(acc);
}

bool BookStore::handleUseradd(const std::vector<std::string>& args) {
    // useradd [UserID] [Password] [Privilege] [Username] - privilege >= 3
    if (!checkPrivilege(3)) return false;
    if (args.size() != 4) return false;

    std::string userid = args[0];
    std::string password = args[1];
    std::string privStr = args[2];
    std::string username = args[3];

    if (!isValidUserId(userid)) return false;
    if (!isValidPassword(password)) return false;
    if (!isValidPrivilege(privStr)) return false;
    if (!isValidUsername(username)) return false;

    int privilege = privStr[0] - '0';

    // Cannot create account with privilege >= current privilege
    if (privilege >= getCurrentPrivilege()) return false;

    if (accountExists(userid)) return false;

    Account acc(userid, password, username, privilege);
    return addAccount(acc);
}

bool BookStore::handleDelete(const std::vector<std::string>& args) {
    // delete [UserID] - privilege 7
    if (!checkPrivilege(7)) return false;
    if (args.size() != 1) return false;

    std::string userid = args[0];
    if (!isValidUserId(userid)) return false;

    if (!accountExists(userid)) return false;
    if (isUserLoggedIn(userid)) return false;

    return deleteAccount(userid);
}

bool BookStore::handleShow(const std::vector<std::string>& args) {
    // show (-ISBN=[ISBN] | -name="[BookName]" | -author="[Author]" | -keyword="[Keyword]")? - privilege >= 1
    if (!checkPrivilege(1)) return false;
    if (args.size() > 1) return false;

    std::vector<Book> result;

    if (args.empty()) {
        result = getAllBooks();
    } else {
        std::string arg = args[0];
        size_t eqPos = arg.find('=');
        if (eqPos == std::string::npos) return false;
        std::string type = arg.substr(1, eqPos - 1);
        std::string value = arg.substr(eqPos + 1);

        // Strip surrounding quotes if present
        if (!value.empty() && value.front() == '"' && value.back() == '"') {
            value = value.substr(1, value.size() - 2);
        }

        if (value.empty()) return false;

        if (type == "ISBN") {
            if (!isValidISBN(value)) return false;
            result = searchBooksByISBN(value);
        } else if (type == "name") {
            if (!isValidBookName(value)) return false;
            result = searchBooksByName(value);
        } else if (type == "author") {
            if (!isValidAuthor(value)) return false;
            result = searchBooksByAuthor(value);
        } else if (type == "keyword") {
            if (!isValidKeyword(value)) return false;
            // Check if contains multiple keywords - according to spec, operation fails if multiple
            if (value.find('|') != std::string::npos) return false;
            result = searchBooksByKeyword(value);
        } else {
            return false;
        }
    }

    // Sort by ISBN lex order
    std::sort(result.begin(), result.end(), [](const Book& a, const Book& b) {
        return a.isbn < b.isbn;
    });

    for (const auto& b : result) {
        std::cout << b.isbn << "\t"
                  << b.name << "\t"
                  << b.author << "\t"
                  << b.keyword << "\t"
                  << std::fixed << std::setprecision(2) << b.price << "\t"
                  << b.stock << "\n";
    }

    // Always output something - if no matches, output empty line according to spec
    if (result.empty()) {
        std::cout << "\n";
    }

    return true;
}

bool BookStore::handleBuy(const std::vector<std::string>& args) {
    // buy [ISBN] [Quantity] - privilege >= 1
    if (!checkPrivilege(1)) return false;
    if (args.size() != 2) return false;

    std::string isbn = args[0];
    std::string qtyStr = args[1];

    if (!isValidISBN(isbn)) return false;
    if (!isValidQuantity(qtyStr)) return false;

    int quantity = std::stoi(qtyStr);

    Book book;
    if (!findBook(isbn, book)) return false;
    if (book.stock < quantity) return false;

    double total = book.price * quantity;

    // Reduce stock
    book.stock -= quantity;
    updateBook(book);

    // Log transaction - income (positive amount)
    Transaction trans(0, getCurrentAccount().username, "buy", isbn, quantity, total);
    addTransaction(trans);

    std::cout << std::fixed << std::setprecision(2) << total << "\n";
    return true;
}

bool BookStore::handleSelect(const std::vector<std::string>& args) {
    // select [ISBN] - privilege >= 3
    if (!checkPrivilege(3)) return false;
    if (args.size() != 1) return false;

    std::string isbn = args[0];
    if (!isValidISBN(isbn)) return false;

    selectedISBN = isbn;

    if (!bookExists(isbn)) {
        Book newBook(isbn);
        addBook(newBook);
    }

    return true;
}

bool BookStore::handleModify(const std::vector<std::string>& args) {
    // modify (-ISBN=[ISBN] | -name="[BookName]" | -author="[Author]" | -keyword="[Keyword]" | -price=[Price])+ - privilege >= 3
    if (!checkPrivilege(3)) return false;
    if (args.empty()) return false;
    if (selectedISBN.empty()) return false;

    // Check that no book is currently selected or selected book doesn't exist
    Book currentBook;
    if (!findBook(selectedISBN, currentBook)) return false;

    // Check for duplicate types
    std::vector<std::string> typesFound;
    std::map<std::string, std::string> modifications;

    for (const auto& arg : args) {
        std::string type, value;
        if (!parseModifyArg(arg, type, value)) return false;

        // Strip surrounding quotes if present
        if (!value.empty() && value.front() == '"' && value.back() == '"') {
            value = value.substr(1, value.size() - 2);
        }

        // Check for duplicate types
        for (const auto& t : typesFound) {
            if (t == type) return false;
        }
        typesFound.push_back(type);

        modifications[type] = value;
    }

    // Validate each change
    std::string newISBN = currentBook.isbn;
    std::string newName = currentBook.name;
    std::string newAuthor = currentBook.author;
    std::string newKeyword = currentBook.keyword;
    double newPrice = currentBook.price;

    if (modifications.count("ISBN")) {
        newISBN = modifications["ISBN"];
        if (!isValidISBN(newISBN)) return false;
        if (newISBN != currentBook.isbn && bookExists(newISBN)) return false;
        // Cannot change to the same ISBN
        if (newISBN == currentBook.isbn) return false;
    }

    if (modifications.count("name")) {
        newName = modifications["name"];
        if (!isValidBookName(newName)) return false;
    }

    if (modifications.count("author")) {
        newAuthor = modifications["author"];
        if (!isValidAuthor(newAuthor)) return false;
    }

    if (modifications.count("keyword")) {
        newKeyword = modifications["keyword"];
        if (!isValidKeyword(newKeyword)) return false;
        if (hasDuplicateKeywords(newKeyword)) return false;
    }

    if (modifications.count("price")) {
        if (!isValidPrice(modifications["price"])) return false;
        newPrice = std::stod(modifications["price"]);
    }

    // Remove the old book if ISBN changed
    if (newISBN != currentBook.isbn) {
        // Since we already checked newISBN doesn't exist
        deleteBook(selectedISBN);
    }

    // Create updated book
    Book updated;
    updated.isbn = newISBN;
    updated.name = newName;
    updated.author = newAuthor;
    updated.keyword = newKeyword;
    updated.price = newPrice;
    updated.stock = currentBook.stock;

    if (newISBN == currentBook.isbn) {
        updateBook(updated);
    } else {
        addBook(updated);
        selectedISBN = newISBN;
    }

    return true;
}

bool BookStore::handleImport(const std::vector<std::string>& args) {
    // import [Quantity] [TotalCost] - privilege >= 3
    if (!checkPrivilege(3)) return false;
    if (args.size() != 2) return false;
    if (selectedISBN.empty()) return false;

    std::string qtyStr = args[0];
    std::string costStr = args[1];

    if (!isValidQuantity(qtyStr)) return false;
    if (!isValidPrice(costStr)) return false;

    int quantity = std::stoi(qtyStr);
    double totalCost = std::stod(costStr);
    if (totalCost <= 0) return false;

    Book book;
    if (!findBook(selectedISBN, book)) return false;

    // Increase stock
    book.stock += quantity;
    updateBook(book);

    // Log transaction - expenditure (negative amount)
    Transaction trans(0, getCurrentAccount().username, "import", book.isbn, quantity, -totalCost);
    addTransaction(trans);

    return true;
}

bool BookStore::handleShowFinance(const std::vector<std::string>& args) {
    // show finance ([Count])? - privilege 7
    if (!checkPrivilege(7)) return false;
    if (args.size() > 1) return false;

    std::vector<Transaction> transactions;
    int totalCount = getTransactionCount();

    if (args.empty()) {
        transactions = getAllTransactions();
    } else {
        std::string countStr = args[0];
        if (!isValidCount(countStr)) return false;
        int count = std::stoi(countStr);
        if (count == 0) {
            std::cout << "\n";
            return true;
        }
        if (count > totalCount) return false;
        transactions = getLastTransactions(count);
    }

    double income = 0.0;
    double expenditure = 0.0;

    for (const auto& t : transactions) {
        if (t.amount > 0) {
            income += t.amount;
        } else {
            expenditure += (-t.amount);
        }
    }

    std::cout << "+ " << std::fixed << std::setprecision(2) << income
              << " - " << std::fixed << std::setprecision(2) << expenditure << "\n";

    return true;
}

bool BookStore::handleLog() {
    // log - privilege 7 - output all transactions
    if (!checkPrivilege(7)) return false;

    auto transactions = getAllTransactions();
    for (const auto& t : transactions) {
        std::string opDesc;
        if (t.operation == "buy") {
            opDesc = "bought " + std::to_string(t.quantity) + " of " + t.isbn + " for " +
                     std::to_string(t.amount);
        } else if (t.operation == "import") {
            opDesc = "imported " + std::to_string(t.quantity) + " of " + t.isbn + " for " +
                     std::to_string(-t.amount);
        }
        std::cout << t.username << ": " << opDesc << "\n";
    }

    return true;
}

bool BookStore::handleReportFinance() {
    // report finance - privilege 7
    if (!checkPrivilege(7)) return false;

    auto transactions = getAllTransactions();
    double income = 0.0;
    double expenditure = 0.0;

    for (const auto& t : transactions) {
        if (t.amount > 0) {
            income += t.amount;
        } else {
            expenditure += (-t.amount);
        }
    }

    std::cout << "===== Financial Report =====\n";
    std::cout << "Total Transactions: " << transactions.size() << "\n";
    std::cout << "  Total Income: " << std::fixed << std::setprecision(2) << income << "\n";
    std::cout << "  Total Expenditure: " << std::fixed << std::setprecision(2) << expenditure << "\n";
    std::cout << "  Net Profit: " << std::fixed << std::setprecision(2) << (income - expenditure) << "\n";
    std::cout << "=============================\n";

    return true;
}

bool BookStore::handleReportEmployee() {
    // report employee - privilege 7
    if (!checkPrivilege(7)) return false;

    auto transactions = getAllTransactions();
    std::map<std::string, int> opCount;
    for (const auto& t : transactions) {
        opCount[t.username]++;
    }

    std::cout << "===== Employee Report =====\n";
    std::cout << "Total Operations by Employee:\n";
    for (const auto& entry : opCount) {
        std::cout << "  " << entry.first << ": " << entry.second << " operations\n";
    }
    std::cout << "============================\n";

    return true;
}

bool BookStore::handleQuit() {
    // logout all accounts, clear selected book
    while (!loginStack.empty()) {
        loginStack.pop();
    }
    selectedISBN.clear();
    closeFiles();
    return false;  // exit program
}

bool BookStore::deleteBook(const std::string& isbn) {
    std::lock_guard<std::mutex> lock(fileMutex);
    if (!bookIndex.count(isbn)) return false;

    std::vector<std::string> lines;
    booksStream.clear();
    booksStream.seekg(0);

    std::string line;
    bool found = false;
    while (std::getline(booksStream, line)) {
        if (line.empty()) continue;

        size_t firstBar = line.find('|');
        if (firstBar == std::string::npos) {
            lines.push_back(line);
            continue;
        }

        int isbnSize = std::stoi(line.substr(0, firstBar));
        size_t pos = firstBar + 1;
        std::string ib = line.substr(pos, isbnSize);

        if (ib != isbn) {
            lines.push_back(line + "\n");
        } else {
            found = true;
        }
    }

    if (!found) return false;

    bookIndex.clear();
    booksStream.close();
    std::ofstream out(booksFile, std::ios::trunc | std::ios::binary);
    std::streamoff offset = 0;
    for (const auto& l : lines) {
        out << l;
        std::string::size_type lineLen = l.size();
        // Find the isbn in this line for index
        size_t firstBar = l.find('|');
        if (firstBar != std::string::npos) {
            int isbnSize = std::stoi(l.substr(0, firstBar));
            size_t pos = firstBar + 1;
            std::string ib = l.substr(pos, isbnSize);
            bookIndex[ib] = {offset, (int)lineLen};
        }
        offset += lineLen;
    }
    out.flush();
    out.close();

    booksStream.open(booksFile, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);

    return booksStream.is_open();
}
