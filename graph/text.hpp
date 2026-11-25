#pragma once

#include <charconv>
#include <cmath>
#include <filesystem>
#include <iosfwd>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>

namespace frontier {

template<class Number>
Number parse_number(std::string_view token, std::string_view name) {
    if (token.empty()) {
        throw std::invalid_argument("missing " + std::string(name));
    }
    Number value{};
    const auto decoded = std::from_chars(token.data(), token.data() + token.size(), value);
    if (decoded.ec != std::errc{} || decoded.ptr != token.data() + token.size()) {
        throw std::invalid_argument("invalid " + std::string(name) + ": " + std::string(token));
    }
    if constexpr (std::is_floating_point_v<Number>) {
        if (!std::isfinite(value)) {
            throw std::invalid_argument("nonfinite " + std::string(name));
        }
    }
    return value;
}

class TextReader {
public:
    explicit TextReader(std::istream& stream, std::string_view format)
        : stream_(stream), format_(format) {}

    std::string token(std::string_view name);
    void expect(std::string_view label);
    void finish();

    template<class Number>
    Number number(std::string_view name) {
        return parse_number<Number>(token(name), name);
    }

private:
    std::istream& stream_;
    std::string format_;
};

void write_new_text_file(const std::filesystem::path& file, std::string_view contents);

}  // namespace frontier
