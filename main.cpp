#include <iostream>
#include <string>
#include <chrono>

// Forward declarations
void query1(const std::string& filename);
void query2(const std::string& filename);
void query3(const std::string& filename);
void query4(const std::string& filename);

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: ./query_engine <query1|query2|query3|query4> <input_file>" << std::endl;
        return 1;
    }

    std::string query = argv[1];
    std::string filename = argv[2];

    // Record start time
    auto start = std::chrono::high_resolution_clock::now();

    try {
        if (query == "query1") {
            query1(filename);
        } else if (query == "query2") {
            query2(filename);
        } else if (query == "query3") {
            query3(filename);
        } else if (query == "query4") {
            query4(filename);
        } else {
            std::cerr << "Invalid query specified." << std::endl;
            return 1;
        }

        // Record end time and print duration
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cerr << "\nQuery completed in " << duration.count() << "ms" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
