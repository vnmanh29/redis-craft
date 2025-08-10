//
// Created by Manh Nguyen Viet on 7/17/25.
//

#ifndef REDIS_STARTER_CPP_GLOBMATCHER_HPP
#define REDIS_STARTER_CPP_GLOBMATCHER_HPP

#include <string>
#include <iostream>
#include <regex>
#include <vector>

class GlobMatcher {
private:
    // Convert glob pattern to regex pattern
    static std::string globToRegex(const std::string& glob) {
        std::string regex = "^"; // Start anchor

        for (size_t i = 0; i < glob.length(); i++) {
            char c = glob[i];

            switch (c) {
                case '*':
                    regex += ".*";
                    break;

                case '?':
                    regex += ".";
                    break;

                case '[':
                    // Handle bracket expressions
                    regex += '[';
                    i++; // Move past '['

                    // Check for negation
                    if (i < glob.length() && (glob[i] == '!' || glob[i] == '^')) {
                        regex += '^'; // Convert both ! and ^ to ^ for regex
                        i++;
                    }

                    // Copy the bracket content
                    while (i < glob.length() && glob[i] != ']') {
                        char bracketChar = glob[i];

                        // Escape special regex characters within brackets
                        if (bracketChar == '\\' || bracketChar == '^' || bracketChar == '-') {
                            regex += '\\';
                        }

                        regex += bracketChar;
                        i++;
                    }

                    if (i < glob.length() && glob[i] == ']') {
                        regex += ']';
                    } else {
                        // Malformed bracket expression, treat '[' as literal
                        regex = regex.substr(0, regex.length() - 1); // Remove the '['
                        regex += "\\[";
                        i = glob.find('[', 0) + 1; // Reset to after the '['
                        i--; // Will be incremented by loop
                    }
                    break;

                case ']':
                    // Standalone ']' should be escaped
                    regex += "\\]";
                    break;

                case '.':
                case '^':
                case '$':
                case '+':
                case '{':
                case '}':
                case '(':
                case ')':
                case '|':
                case '\\':
                    // Escape special regex characters
                    regex += '\\';
                    regex += c;
                    break;

                default:
                    // Regular character
                    regex += c;
                    break;
            }
        }

        regex += "$"; // End anchor
        return regex;
    }

public:
    static bool matchGlob(const std::string& text, const std::string& pattern) {
        try {
            std::string regexPattern = globToRegex(pattern);
            std::regex globRegex(regexPattern);
            return std::regex_match(text, globRegex);
        } catch (const std::regex_error& e) {
            // If regex compilation fails, return false
            return false;
        }
    }

    // Additional utility method to see the converted regex (for debugging)
    static std::string getRegexPattern(const std::string& pattern) {
        return globToRegex(pattern);
    }
};

// Convenience function
bool matchGlob(const std::string& text, const std::string& pattern) {
    return GlobMatcher::matchGlob(text, pattern);
}

#endif //REDIS_STARTER_CPP_GLOBMATCHER_HPP
