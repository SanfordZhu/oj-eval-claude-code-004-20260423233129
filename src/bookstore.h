#ifndef BOOKSTORE_H
#define BOOKSTORE_H

#include <string>
#include <vector>
#include <stack>
#include <map>
#include <fstream>
#include <mutex>

class Account {
public:
    std::string userid;
    std::string password;
    std::string username;
    int privilege; // 0: guest, 1: customer, 3: employee, 7: owner

    Account() : privilege(0) {}
    Account(const std::string& uid, const std::string& pwd, const std::string& uname, int priv)
        : userid(uid), password(pwd), username(uname), privilege(priv) {}

    bool isValid() const { return !userid.empty(); }
};

class Book {
public:
    std::string isbn;
    std::string name;
    std::string author;
    std::string keyword; // stored as-is with | separators
    double price;
    int stock;

    Book() : price(0), stock(0) {}
    Book(const std::string& ib) : isbn(ib), price(0), stock(0) {}

    bool isValid() const { return !isbn.empty(); }
};

struct Transaction {
    int id;
    std::string username;
    std::string operation;  // "buy" or "import"
    std::string isbn;
    int quantity;
    double amount;  // positive for income (buy), negative for expenditure (import)
    long long timestamp;

    Transaction() : id(0), quantity(0), amount(0), timestamp(0) {}
    Transaction(int i, const std::string& uname, const std::string& op, const std::string& ib, int q, double a)
        : id(i), username(uname), operation(op), isbn(ib), quantity(q), amount(a) {}
};

class BookStore {
private:
    std::string dataDir;
    std::string accountsFile;
    std::string booksFile;
    std::string transactionsFile;
    std::string counterFile;

    std::stack<Account> loginStack;
    std::string selectedISBN;

    int nextTransactionId;

    bool initialized;

    std::fstream accountsStream;
    std::fstream booksStream;
    std::fstream transactionsStream;

    std::mutex fileMutex;

public:
    BookStore(const std::string& dir = "./data");
    ~BookStore();

    bool init();
    bool processCommand(const std::string& line);

private:
    // File operations
    bool openFiles();
    void closeFiles();
    bool createDefaultFiles();
    int readNextTransactionId();
    void writeNextTransactionId(int id);

    // Account operations
    bool findAccount(const std::string& userid, Account& out);
    bool addAccount(const Account& account);
    bool deleteAccount(const std::string& userid);
    bool updateAccount(const Account& account);
    bool accountExists(const std::string& userid);
    bool isUserLoggedIn(const std::string& userid);

    // Book operations
    bool findBook(const std::string& isbn, Book& out);
    bool addBook(const Book& book);
    bool updateBook(const Book& book);
    bool deleteBook(const std::string& isbn);
    bool bookExists(const std::string& isbn);
    std::vector<Book> searchBooksByISBN(const std::string& isbn);
    std::vector<Book> searchBooksByName(const std::string& name);
    std::vector<Book> searchBooksByAuthor(const std::string& author);
    std::vector<Book> searchBooksByKeyword(const std::string& keyword);
    std::vector<Book> getAllBooks();

    // Transaction operations
    bool addTransaction(const Transaction& trans);
    std::vector<Transaction> getLastTransactions(int count);
    std::vector<Transaction> getAllTransactions();
    int getTransactionCount();

    // Command handlers
    bool handleSu(const std::vector<std::string>& args);
    bool handleLogout();
    bool handleRegister(const std::vector<std::string>& args);
    bool handlePasswd(const std::vector<std::string>& args);
    bool handleUseradd(const std::vector<std::string>& args);
    bool handleDelete(const std::vector<std::string>& args);
    bool handleShow(const std::vector<std::string>& args);
    bool handleBuy(const std::vector<std::string>& args);
    bool handleSelect(const std::vector<std::string>& args);
    bool handleModify(const std::vector<std::string>& args);
    bool handleImport(const std::vector<std::string>& args);
    bool handleShowFinance(const std::vector<std::string>& args);
    bool handleLog();
    bool handleReportFinance();
    bool handleReportEmployee();
    bool handleQuit();

    // Validation helpers
    bool checkPrivilege(int required) const;
    bool isValidUserId(const std::string& s) const;
    bool isValidPassword(const std::string& s) const;
    bool isValidPrivilege(const std::string& s) const;
    bool isValidUsername(const std::string& s) const;
    bool isValidISBN(const std::string& s) const;
    bool isValidBookName(const std::string& s) const;
    bool isValidAuthor(const std::string& s) const;
    bool isValidKeyword(const std::string& s) const;
    bool isValidQuantity(const std::string& s) const;
    bool isValidPrice(const std::string& s) const;
    bool isValidCount(const std::string& s) const;
    bool hasDuplicateKeywords(const std::string& keyword) const;

    // Parsing helpers
    std::vector<std::string> splitLine(const std::string& line) const;
    bool parseModifyArg(const std::string& arg, std::string& type, std::string& value) const;

    // Current state
    Account getCurrentAccount() const;
    int getCurrentPrivilege() const;
};

#endif // BOOKSTORE_H
