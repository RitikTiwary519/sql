#ifndef TRIPRECORD_HPP
#define TRIPRECORD_HPP

#include <string>
#include <string_view>
#include <cstring>

// Optimized structure for faster processing
struct TripRecord {
    // Use integer types for categorical data
    int32_t VendorID = 0;
    int32_t Payment_type = 0;
    char Store_and_fwd_flag = 'N';
    
    // Fixed-size buffer for date to avoid string allocations
    char date[11] = {0}; // YYYY-MM-DD\0
    
    double Trip_distance = 0.0;
    double fare = 0.0;
    double tip = 0.0;
    int32_t passenger_count = 0;

    // Helper methods for date comparison
    bool isInJanuary2024() const {
        return strncmp(date, "2024-01", 7) == 0;
    }

    // Fast string view based getters
    std::string_view getDate() const { return std::string_view(date); }
};

// Stats structure for aggregations
struct Stats {
    size_t count = 0;
    double fare_sum = 0.0;
    double tip_sum = 0.0;
    double distance_sum = 0.0;
    int32_t passenger_sum = 0;

    // Thread-safe merge operation
    void merge(const Stats& other) {
        count += other.count;
        fare_sum += other.fare_sum;
        tip_sum += other.tip_sum;
        distance_sum += other.distance_sum;
        passenger_sum += other.passenger_sum;
    }
};

#endif
