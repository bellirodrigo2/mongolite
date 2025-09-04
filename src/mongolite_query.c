#include "mongolite_internal.h"
#include <string.h>
#include <stdlib.h>
#include <regex.h>

// MongoDB's BSON type precedence ordering (lower numbers = lower precedence)
// Based on: https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/
int get_mongodb_type_precedence(bson_type_t type) {
    switch (type) {
        case BSON_TYPE_MINKEY:    return 1;   // MinKey (lowest)
        case BSON_TYPE_NULL:      return 2;   // Null
        case BSON_TYPE_INT32:     return 3;   // Numbers
        case BSON_TYPE_INT64:     return 3;   // Numbers (same precedence as int32)
        case BSON_TYPE_DOUBLE:    return 3;   // Numbers (same precedence as int32/int64)
        case BSON_TYPE_UTF8:      return 4;   // String
        case BSON_TYPE_DOCUMENT:  return 5;   // Object
        case BSON_TYPE_ARRAY:     return 6;   // Array
        case BSON_TYPE_BINARY:    return 7;   // BinaryData
        case BSON_TYPE_OID:       return 8;   // ObjectId
        case BSON_TYPE_BOOL:      return 9;   // Boolean
        case BSON_TYPE_DATE_TIME: return 10;  // Date
        case BSON_TYPE_TIMESTAMP: return 11;  // Timestamp
        case BSON_TYPE_REGEX:     return 12;  // Regular Expression
        case BSON_TYPE_MAXKEY:    return 13;  // MaxKey (highest)
        default:                  return 0;   // Unknown type (treat as lowest)
    }
}

// Helper function to get MongoDB type name from BSON type
static const char* get_mongodb_type_name(bson_type_t type) {
    switch (type) {
        case BSON_TYPE_DOUBLE:     return "double";
        case BSON_TYPE_UTF8:       return "string";
        case BSON_TYPE_DOCUMENT:   return "object";
        case BSON_TYPE_ARRAY:      return "array";
        case BSON_TYPE_BINARY:     return "binData";
        case BSON_TYPE_OID:        return "objectId";
        case BSON_TYPE_BOOL:       return "bool";
        case BSON_TYPE_DATE_TIME:  return "date";
        case BSON_TYPE_NULL:       return "null";
        case BSON_TYPE_REGEX:      return "regex";
        case BSON_TYPE_INT32:      return "int";
        case BSON_TYPE_TIMESTAMP:  return "timestamp";
        case BSON_TYPE_INT64:      return "long";
        case BSON_TYPE_MINKEY:     return "minKey";
        case BSON_TYPE_MAXKEY:     return "maxKey";
        default:                   return "unknown";
    }
}

// Helper function to compare two BSON values with full MongoDB semantics
// Returns: -1 (v1 < v2), 0 (v1 == v2), 1 (v1 > v2)
int mongodb_value_compare(const bson_iter_t *iter1, const bson_iter_t *iter2) {
    bson_type_t type1 = bson_iter_type(iter1);
    bson_type_t type2 = bson_iter_type(iter2);
    
    // First, check type precedence
    int precedence1 = get_mongodb_type_precedence(type1);
    int precedence2 = get_mongodb_type_precedence(type2);
    
    if (precedence1 != precedence2) {
        return (precedence1 < precedence2) ? -1 : 1;  // Type precedence determines order
    }
    
    // Same type precedence - compare values semantically
    switch (type1) {
        case BSON_TYPE_MINKEY:
        case BSON_TYPE_MAXKEY:
        case BSON_TYPE_NULL:
            return 0;  // All instances of these types are equal
            
        case BSON_TYPE_INT32:
        case BSON_TYPE_INT64: 
        case BSON_TYPE_DOUBLE: {
            // Convert all numeric types to double for comparison
            double val1, val2;
            
            switch (type1) {
                case BSON_TYPE_INT32:
                    val1 = (double)bson_iter_int32(iter1);
                    break;
                case BSON_TYPE_INT64:
                    val1 = (double)bson_iter_int64(iter1);
                    break;
                case BSON_TYPE_DOUBLE:
                    val1 = bson_iter_double(iter1);
                    break;
                default:
                    return 0;  // Should not reach here
            }
            
            switch (type2) {
                case BSON_TYPE_INT32:
                    val2 = (double)bson_iter_int32(iter2);
                    break;
                case BSON_TYPE_INT64:
                    val2 = (double)bson_iter_int64(iter2);
                    break;
                case BSON_TYPE_DOUBLE:
                    val2 = bson_iter_double(iter2);
                    break;
                default:
                    return 0;  // Should not reach here
            }
            
            if (val1 < val2) return -1;
            if (val1 > val2) return 1;
            return 0;
        }
        
        case BSON_TYPE_UTF8: {
            uint32_t len1, len2;
            const char *str1 = bson_iter_utf8(iter1, &len1);
            const char *str2 = bson_iter_utf8(iter2, &len2);
            
            // Use standard string comparison (MongoDB uses lexicographic ordering)
            int cmp = strcmp(str1, str2);
            return (cmp < 0) ? -1 : (cmp > 0) ? 1 : 0;
        }
        
        case BSON_TYPE_BOOL: {
            bool val1 = bson_iter_bool(iter1);
            bool val2 = bson_iter_bool(iter2);
            if (val1 < val2) return -1;
            if (val1 > val2) return 1;
            return 0;
        }
        
        case BSON_TYPE_OID: {
            const bson_oid_t *oid1 = bson_iter_oid(iter1);
            const bson_oid_t *oid2 = bson_iter_oid(iter2);
            return bson_oid_compare(oid1, oid2);  // Use libbson's OID comparison
        }
        
        case BSON_TYPE_DATE_TIME: {
            int64_t time1 = bson_iter_date_time(iter1);
            int64_t time2 = bson_iter_date_time(iter2);
            if (time1 < time2) return -1;
            if (time1 > time2) return 1;
            return 0;
        }
        
        case BSON_TYPE_DOCUMENT:
        case BSON_TYPE_ARRAY: {
            // For complex types, create temporary documents and use bson_compare
            // This is a simplified approach - full MongoDB compatibility would need recursive comparison
            const uint8_t *data1, *data2;
            uint32_t len1, len2;
            
            if (type1 == BSON_TYPE_DOCUMENT) {
                bson_iter_document(iter1, &len1, &data1);
                bson_iter_document(iter2, &len2, &data2);
            } else {
                bson_iter_array(iter1, &len1, &data1);
                bson_iter_array(iter2, &len2, &data2);
            }
            
            bson_t doc1, doc2;
            if (bson_init_static(&doc1, data1, len1) && bson_init_static(&doc2, data2, len2)) {
                return bson_compare(&doc1, &doc2);  // Fall back to byte comparison for now
            }
            return 0;
        }
        
        default:
            // For other types (binary, regex, timestamp), fall back to type precedence
            // In a full implementation, we'd need specific comparison logic for each type
            return 0;
    }
}

// Helper function to compare BSON iterator values with MongoDB-style cross-type support
// Uses the full MongoDB comparison logic for equality
bool bson_iter_values_equal(const bson_iter_t *iter1, const bson_iter_t *iter2) {
    return mongodb_value_compare(iter1, iter2) == 0;
}

// Helper function for MongoDB-compatible comparison (returns -1, 0, 1)
// Supports cross-type comparisons with proper precedence ordering
int bson_iter_mongodb_compare(const bson_iter_t *iter1, const bson_iter_t *iter2) {
    return mongodb_value_compare(iter1, iter2);
}

// Query operator type enumeration
typedef enum {
    OP_UNKNOWN = 0,
    OP_EQ,
    OP_NE,
    OP_GT,
    OP_GTE,
    OP_LT,
    OP_LTE,
    OP_IN,
    OP_NIN,
    OP_EXISTS,
    OP_TYPE,
    OP_ALL,
    OP_SIZE,
    OP_AND,
    OP_OR,
    OP_NOT,
    OP_NOR,
    OP_REGEX
} query_operator_t;

// Static helper functions for complex operators
static bool evaluate_in_operator(const bson_iter_t *doc_iter, const bson_iter_t *query_iter);
static bool evaluate_nin_operator(const bson_iter_t *doc_iter, const bson_iter_t *query_iter);
static bool evaluate_exists_operator(const bson_iter_t *doc_iter, const bson_iter_t *query_iter);
static bool evaluate_type_operator(const bson_iter_t *doc_iter, const bson_iter_t *query_iter);
static bool evaluate_all_operator(const bson_iter_t *doc_iter, const bson_iter_t *query_iter);
static bool evaluate_size_operator(const bson_iter_t *doc_iter, const bson_iter_t *query_iter);
static bool evaluate_and_operator(const bson_t *doc, const bson_iter_t *query_iter);
static bool evaluate_or_operator(const bson_t *doc, const bson_iter_t *query_iter);
static bool evaluate_not_operator(const bson_t *doc, const bson_iter_t *query_iter);
static bool evaluate_nor_operator(const bson_t *doc, const bson_iter_t *query_iter);
static bool evaluate_regex_operator(const bson_iter_t *doc_iter, const bson_iter_t *query_iter);
static query_operator_t parse_query_operator(const char *op_string);

// Helper function to evaluate query operators
bool evaluate_query_operator(const bson_iter_t *doc_iter, const bson_t *query_expr) {
    bson_iter_t query_iter;
    if (!bson_iter_init(&query_iter, query_expr)) {
        return false;
    }
    
    while (bson_iter_next(&query_iter)) {
        const char *op = bson_iter_key(&query_iter);
        query_operator_t op_type = parse_query_operator(op);
        
        switch (op_type) {
            case OP_EQ:
                return bson_iter_values_equal(doc_iter, &query_iter);
                
            case OP_NE:
                return !bson_iter_values_equal(doc_iter, &query_iter);
                
            case OP_GT: {
                int cmp = bson_iter_mongodb_compare(doc_iter, &query_iter);
                return cmp > 0;
            }
            
            case OP_GTE: {
                int cmp = bson_iter_mongodb_compare(doc_iter, &query_iter);
                return cmp >= 0;
            }
            
            case OP_LT: {
                int cmp = bson_iter_mongodb_compare(doc_iter, &query_iter);
                return cmp < 0;
            }
            
            case OP_LTE: {
                int cmp = bson_iter_mongodb_compare(doc_iter, &query_iter);
                return cmp <= 0;
            }
            
            case OP_IN:
                return evaluate_in_operator(doc_iter, &query_iter);
                
            case OP_NIN:
                return evaluate_nin_operator(doc_iter, &query_iter);
                
            case OP_EXISTS:
                return evaluate_exists_operator(doc_iter, &query_iter);
                
            case OP_TYPE:
                return evaluate_type_operator(doc_iter, &query_iter);
                
            case OP_ALL:
                return evaluate_all_operator(doc_iter, &query_iter);
                
            case OP_SIZE:
                return evaluate_size_operator(doc_iter, &query_iter);
                
            case OP_REGEX:
                return evaluate_regex_operator(doc_iter, &query_iter);
                
            case OP_UNKNOWN:
            default:
                return false; // Unknown operator
        }
    }
    
    return false; // Unknown operator
}

// Parse query operator string to enum
static query_operator_t parse_query_operator(const char *op_string) {
    if (strcmp(op_string, "$eq") == 0) return OP_EQ;
    if (strcmp(op_string, "$ne") == 0) return OP_NE;
    if (strcmp(op_string, "$gt") == 0) return OP_GT;
    if (strcmp(op_string, "$gte") == 0) return OP_GTE;
    if (strcmp(op_string, "$lt") == 0) return OP_LT;
    if (strcmp(op_string, "$lte") == 0) return OP_LTE;
    if (strcmp(op_string, "$in") == 0) return OP_IN;
    if (strcmp(op_string, "$nin") == 0) return OP_NIN;
    if (strcmp(op_string, "$exists") == 0) return OP_EXISTS;
    if (strcmp(op_string, "$type") == 0) return OP_TYPE;
    if (strcmp(op_string, "$all") == 0) return OP_ALL;
    if (strcmp(op_string, "$size") == 0) return OP_SIZE;
    if (strcmp(op_string, "$and") == 0) return OP_AND;
    if (strcmp(op_string, "$or") == 0) return OP_OR;
    if (strcmp(op_string, "$not") == 0) return OP_NOT;
    if (strcmp(op_string, "$nor") == 0) return OP_NOR;
    if (strcmp(op_string, "$regex") == 0) return OP_REGEX;
    return OP_UNKNOWN;
}

// Static helper function for $in operator
static bool evaluate_in_operator(const bson_iter_t *doc_iter, const bson_iter_t *query_iter) {
    if (!BSON_ITER_HOLDS_ARRAY(query_iter)) {
        return false; // $in value must be an array
    }
    
    const uint8_t *array_data;
    uint32_t array_len;
    bson_iter_array(query_iter, &array_len, &array_data);
    
    bson_t array_doc;
    if (!bson_init_static(&array_doc, array_data, array_len)) {
        return false;
    }
    
    bson_iter_t array_iter;
    if (!bson_iter_init(&array_iter, &array_doc)) {
        return false;
    }
    
    while (bson_iter_next(&array_iter)) {
        if (bson_iter_values_equal(doc_iter, &array_iter)) {
            return true;
        }
    }
    
    return false;
}

// Static helper function for $nin operator
static bool evaluate_nin_operator(const bson_iter_t *doc_iter, const bson_iter_t *query_iter) {
    if (!BSON_ITER_HOLDS_ARRAY(query_iter)) {
        return false; // $nin value must be an array
    }
    
    const uint8_t *array_data;
    uint32_t array_len;
    bson_iter_array(query_iter, &array_len, &array_data);
    
    bson_t array_doc;
    if (!bson_init_static(&array_doc, array_data, array_len)) {
        return false;
    }
    
    bson_iter_t array_iter;
    if (!bson_iter_init(&array_iter, &array_doc)) {
        return false;
    }
    
    while (bson_iter_next(&array_iter)) {
        if (bson_iter_values_equal(doc_iter, &array_iter)) {
            return false; // Found a match, so NOT in fails
        }
    }
    
    return true; // No match found, so NOT in succeeds
}

// Static helper function for $exists operator
static bool evaluate_exists_operator(const bson_iter_t *doc_iter, const bson_iter_t *query_iter) {
    bool should_exist = bson_iter_as_bool(query_iter);
    bool field_exists = (bson_iter_type(doc_iter) != BSON_TYPE_EOD);
    return field_exists == should_exist;
}

// Static helper function for $type operator
static bool evaluate_type_operator(const bson_iter_t *doc_iter, const bson_iter_t *query_iter) {
    bson_type_t doc_type = bson_iter_type(doc_iter);
    
    if (bson_iter_type(query_iter) == BSON_TYPE_INT32) {
        // Numeric type code
        int32_t expected_type_code = bson_iter_int32(query_iter);
        return (int32_t)doc_type == expected_type_code;
    }
    else if (bson_iter_type(query_iter) == BSON_TYPE_UTF8) {
        // String type name
        const char *expected_type_name = bson_iter_utf8(query_iter, NULL);
        const char *actual_type_name = get_mongodb_type_name(doc_type);
        return strcmp(expected_type_name, actual_type_name) == 0;
    }
    else if (bson_iter_type(query_iter) == BSON_TYPE_ARRAY) {
        // Array of type codes/names
        const uint8_t *array_data;
        uint32_t array_len;
        bson_iter_array(query_iter, &array_len, &array_data);
        
        bson_t array_doc;
        if (bson_init_static(&array_doc, array_data, array_len)) {
            bson_iter_t array_iter;
            if (bson_iter_init(&array_iter, &array_doc)) {
                while (bson_iter_next(&array_iter)) {
                    if (bson_iter_type(&array_iter) == BSON_TYPE_INT32) {
                        int32_t expected_type_code = bson_iter_int32(&array_iter);
                        if ((int32_t)doc_type == expected_type_code) {
                            return true;
                        }
                    }
                    else if (bson_iter_type(&array_iter) == BSON_TYPE_UTF8) {
                        const char *expected_type_name = bson_iter_utf8(&array_iter, NULL);
                        const char *actual_type_name = get_mongodb_type_name(doc_type);
                        if (strcmp(expected_type_name, actual_type_name) == 0) {
                            return true;
                        }
                    }
                }
            }
        }
        return false; // No matching type found in array
    }
    
    return false; // Invalid $type query format
}

// Static helper function for $all operator
static bool evaluate_all_operator(const bson_iter_t *doc_iter, const bson_iter_t *query_iter) {
    if (!BSON_ITER_HOLDS_ARRAY(doc_iter)) {
        return false; // $all can only be applied to array fields
    }
    
    if (!BSON_ITER_HOLDS_ARRAY(query_iter)) {
        return false; // $all value must be an array
    }
    
    // Get the query array (values to check for)
    const uint8_t *query_array_data;
    uint32_t query_array_len;
    bson_iter_array(query_iter, &query_array_len, &query_array_data);
    
    bson_t query_array_doc;
    if (!bson_init_static(&query_array_doc, query_array_data, query_array_len)) {
        return false;
    }
    
    bson_iter_t query_array_iter;
    if (!bson_iter_init(&query_array_iter, &query_array_doc)) {
        return false;
    }
    
    while (bson_iter_next(&query_array_iter)) {
        bool found = false;
        
        // Get the document array
        const uint8_t *doc_array_data;
        uint32_t doc_array_len;
        bson_iter_array(doc_iter, &doc_array_len, &doc_array_data);
        
        bson_t doc_array_doc;
        if (bson_init_static(&doc_array_doc, doc_array_data, doc_array_len)) {
            bson_iter_t doc_array_iter;
            if (bson_iter_init(&doc_array_iter, &doc_array_doc)) {
                while (bson_iter_next(&doc_array_iter)) {
                    if (bson_iter_values_equal(&query_array_iter, &doc_array_iter)) {
                        found = true;
                        break;
                    }
                }
            }
        }
        
        if (!found) {
            return false; // At least one value not found
        }
    }
    
    return true; // All values found
}

// Static helper function for $size operator
static bool evaluate_size_operator(const bson_iter_t *doc_iter, const bson_iter_t *query_iter) {
    if (!BSON_ITER_HOLDS_ARRAY(doc_iter)) {
        return false; // $size can only be applied to array fields
    }
    
    if (bson_iter_type(query_iter) != BSON_TYPE_INT32) {
        return false; // $size value must be a number
    }
    
    int32_t expected_size = bson_iter_int32(query_iter);
    if (expected_size < 0) {
        return false; // Array size cannot be negative
    }
    
    // Count elements in document array
    const uint8_t *doc_array_data;
    uint32_t doc_array_len;
    bson_iter_array(doc_iter, &doc_array_len, &doc_array_data);
    
    bson_t doc_array_doc;
    if (!bson_init_static(&doc_array_doc, doc_array_data, doc_array_len)) {
        return false;
    }
    
    bson_iter_t doc_array_iter;
    if (!bson_iter_init(&doc_array_iter, &doc_array_doc)) {
        return false;
    }
    
    int32_t actual_size = 0;
    while (bson_iter_next(&doc_array_iter)) {
        actual_size++;
    }
    
    return actual_size == expected_size;
}

// Static helper function for $and operator
static bool evaluate_and_operator(const bson_t *doc, const bson_iter_t *query_iter) {
    if (!BSON_ITER_HOLDS_ARRAY(query_iter)) {
        return false; // $and value must be an array
    }
    
    // Get the array of conditions
    const uint8_t *array_data;
    uint32_t array_len;
    bson_iter_array(query_iter, &array_len, &array_data);
    
    bson_t array_doc;
    if (!bson_init_static(&array_doc, array_data, array_len)) {
        return false;
    }
    
    bson_iter_t array_iter;
    if (!bson_iter_init(&array_iter, &array_doc)) {
        return false;
    }
    
    // All conditions must be true
    while (bson_iter_next(&array_iter)) {
        if (!BSON_ITER_HOLDS_DOCUMENT(&array_iter)) {
            return false; // Each condition must be a document
        }
        
        // Extract the condition document
        const uint8_t *condition_data;
        uint32_t condition_len;
        bson_iter_document(&array_iter, &condition_len, &condition_data);
        
        bson_t condition_doc;
        if (!bson_init_static(&condition_doc, condition_data, condition_len)) {
            return false;
        }
        
        // Recursively evaluate the condition against the document
        if (!document_matches_filter(doc, &condition_doc)) {
            return false; // If any condition fails, $and fails
        }
    }
    
    return true; // All conditions passed
}

// Static helper function for $or operator
static bool evaluate_or_operator(const bson_t *doc, const bson_iter_t *query_iter) {
    if (!BSON_ITER_HOLDS_ARRAY(query_iter)) {
        return false; // $or value must be an array
    }
    
    // Get the array of conditions
    const uint8_t *array_data;
    uint32_t array_len;
    bson_iter_array(query_iter, &array_len, &array_data);
    
    bson_t array_doc;
    if (!bson_init_static(&array_doc, array_data, array_len)) {
        return false;
    }
    
    bson_iter_t array_iter;
    if (!bson_iter_init(&array_iter, &array_doc)) {
        return false;
    }
    
    // At least one condition must be true
    while (bson_iter_next(&array_iter)) {
        if (!BSON_ITER_HOLDS_DOCUMENT(&array_iter)) {
            continue; // Skip invalid conditions
        }
        
        // Extract the condition document
        const uint8_t *condition_data;
        uint32_t condition_len;
        bson_iter_document(&array_iter, &condition_len, &condition_data);
        
        bson_t condition_doc;
        if (!bson_init_static(&condition_doc, condition_data, condition_len)) {
            continue; // Skip invalid conditions
        }
        
        // Recursively evaluate the condition against the document
        if (document_matches_filter(doc, &condition_doc)) {
            return true; // If any condition passes, $or passes
        }
    }
    
    return false; // No conditions passed
}

// Static helper function for $not operator
static bool evaluate_not_operator(const bson_t *doc, const bson_iter_t *query_iter) {
    // $not can work with both single conditions and documents
    if (BSON_ITER_HOLDS_DOCUMENT(query_iter)) {
        // Extract the condition document
        const uint8_t *condition_data;
        uint32_t condition_len;
        bson_iter_document(query_iter, &condition_len, &condition_data);
        
        bson_t condition_doc;
        if (!bson_init_static(&condition_doc, condition_data, condition_len)) {
            return false;
        }
        
        // Return the negation of the condition
        return !document_matches_filter(doc, &condition_doc);
    }
    
    return false; // Invalid $not condition
}

// Static helper function for $nor operator
static bool evaluate_nor_operator(const bson_t *doc, const bson_iter_t *query_iter) {
    if (!BSON_ITER_HOLDS_ARRAY(query_iter)) {
        return false; // $nor value must be an array
    }
    
    // Get the array of conditions
    const uint8_t *array_data;
    uint32_t array_len;
    bson_iter_array(query_iter, &array_len, &array_data);
    
    bson_t array_doc;
    if (!bson_init_static(&array_doc, array_data, array_len)) {
        return false;
    }
    
    bson_iter_t array_iter;
    if (!bson_iter_init(&array_iter, &array_doc)) {
        return false;
    }
    
    // None of the conditions should be true
    while (bson_iter_next(&array_iter)) {
        if (!BSON_ITER_HOLDS_DOCUMENT(&array_iter)) {
            continue; // Skip invalid conditions
        }
        
        // Extract the condition document
        const uint8_t *condition_data;
        uint32_t condition_len;
        bson_iter_document(&array_iter, &condition_len, &condition_data);
        
        bson_t condition_doc;
        if (!bson_init_static(&condition_doc, condition_data, condition_len)) {
            continue; // Skip invalid conditions
        }
        
        // Recursively evaluate the condition against the document
        if (document_matches_filter(doc, &condition_doc)) {
            return false; // If any condition passes, $nor fails
        }
    }
    
    return true; // None of the conditions passed, so $nor passes
}

// Static helper function for $regex operator
static bool evaluate_regex_operator(const bson_iter_t *doc_iter, const bson_iter_t *query_iter) {
    // Check if the document field contains a string value
    if (!BSON_ITER_HOLDS_UTF8(doc_iter)) {
        return false; // $regex only works on string fields
    }
    
    const char *doc_value = bson_iter_utf8(doc_iter, NULL);
    if (!doc_value) {
        return false;
    }
    
    // The query can be either a string (simple regex) or a document with $regex and $options
    if (BSON_ITER_HOLDS_UTF8(query_iter)) {
        // Simple case: {"field": {"$regex": "pattern"}}
        const char *pattern = bson_iter_utf8(query_iter, NULL);
        if (!pattern) {
            return false;
        }
        
        regex_t regex;
        int flags = REG_EXTENDED; // Use extended regular expressions
        
        // Compile the regular expression
        int compile_result = regcomp(&regex, pattern, flags);
        if (compile_result != 0) {
            return false; // Invalid regex pattern
        }
        
        // Execute the regex match
        int match_result = regexec(&regex, doc_value, 0, NULL, 0);
        
        // Clean up
        regfree(&regex);
        
        return (match_result == 0); // 0 means match found
    }
    
    // Handle document format: {"field": {"$regex": "pattern", "$options": "flags"}}
    if (BSON_ITER_HOLDS_DOCUMENT(query_iter)) {
        const uint8_t *doc_data;
        uint32_t doc_len;
        bson_iter_document(query_iter, &doc_len, &doc_data);
        
        bson_t regex_doc;
        if (!bson_init_static(&regex_doc, doc_data, doc_len)) {
            return false;
        }
        
        bson_iter_t regex_iter;
        const char *pattern = NULL;
        const char *options = NULL;
        
        // Extract pattern and options
        if (bson_iter_init(&regex_iter, &regex_doc)) {
            while (bson_iter_next(&regex_iter)) {
                const char *key = bson_iter_key(&regex_iter);
                if (strcmp(key, "$regex") == 0 && BSON_ITER_HOLDS_UTF8(&regex_iter)) {
                    pattern = bson_iter_utf8(&regex_iter, NULL);
                } else if (strcmp(key, "$options") == 0 && BSON_ITER_HOLDS_UTF8(&regex_iter)) {
                    options = bson_iter_utf8(&regex_iter, NULL);
                }
            }
        }
        
        if (!pattern) {
            return false; // No pattern specified
        }
        
        regex_t regex;
        int flags = REG_EXTENDED; // Default flags
        
        // Process options (MongoDB-style flags)
        if (options) {
            for (const char *opt = options; *opt; opt++) {
                switch (*opt) {
                    case 'i': // Case insensitive
                        flags |= REG_ICASE;
                        break;
                    case 'm': // Multiline - treat ^ and $ as line anchors
                        flags |= REG_NEWLINE;
                        break;
                    // Note: POSIX regex doesn't support 's' (dotall) flag directly
                    // Other MongoDB flags like 'x' (verbose) are also not supported in POSIX
                }
            }
        }
        
        // Compile the regular expression
        int compile_result = regcomp(&regex, pattern, flags);
        if (compile_result != 0) {
            return false; // Invalid regex pattern
        }
        
        // Execute the regex match
        int match_result = regexec(&regex, doc_value, 0, NULL, 0);
        
        // Clean up
        regfree(&regex);
        
        return (match_result == 0); // 0 means match found
    }
    
    return false; // Unsupported query format
}

// Helper function to match BSON document against filter
bool document_matches_filter(const bson_t *doc, const bson_t *filter) {
    if (!filter || bson_empty(filter)) {
        return true;  // Empty filter matches all documents
    }
    
    bson_iter_t filter_iter;
    if (!bson_iter_init(&filter_iter, filter)) {
        return false;
    }
    
    // Check each condition in the filter
    while (bson_iter_next(&filter_iter)) {
        const char *field_name = bson_iter_key(&filter_iter);
        
        // Handle logical operators that operate on the entire document
        if (strcmp(field_name, "$and") == 0) {
            if (!evaluate_and_operator(doc, &filter_iter)) {
                return false;
            }
            continue;
        }
        
        if (strcmp(field_name, "$or") == 0) {
            if (!evaluate_or_operator(doc, &filter_iter)) {
                return false;
            }
            continue;
        }
        
        if (strcmp(field_name, "$not") == 0) {
            if (!evaluate_not_operator(doc, &filter_iter)) {
                return false;
            }
            continue;
        }
        
        if (strcmp(field_name, "$nor") == 0) {
            if (!evaluate_nor_operator(doc, &filter_iter)) {
                return false;
            }
            continue;
        }
        
        // Find the field in the document
        bson_iter_t doc_iter;
        bool field_found = bson_iter_init_find(&doc_iter, doc, field_name);
        
        // For $exists operator, we need to handle missing fields specially
        if (!field_found) {
            // Check if this is an $exists query that might match missing fields
            if (bson_iter_type(&filter_iter) == BSON_TYPE_DOCUMENT) {
                const uint8_t *data;
                uint32_t len;
                bson_iter_document(&filter_iter, &len, &data);
                
                bson_t query_expr;
                if (bson_init_static(&query_expr, data, len)) {
                    bson_iter_t query_iter;
                    if (bson_iter_init(&query_iter, &query_expr) && 
                        bson_iter_next(&query_iter) &&
                        strcmp(bson_iter_key(&query_iter), "$exists") == 0) {
                        // This is an $exists query - evaluate it with a null iterator
                        bool should_exist = bson_iter_as_bool(&query_iter);
                        if (!should_exist) {
                            // Field doesn't exist and we want non-existing fields
                            continue;  // This condition matches
                        }
                    }
                }
            }
            return false;  // Field not found and not handled by $exists
        }
        
        // Check if the filter value is a query operator document or regex
        if (bson_iter_type(&filter_iter) == BSON_TYPE_DOCUMENT) {
            // Extract the query expression document
            const uint8_t *data;
            uint32_t len;
            bson_iter_document(&filter_iter, &len, &data);
            
            bson_t query_expr;
            if (bson_init_static(&query_expr, data, len)) {
                // Evaluate query operators
                if (!evaluate_query_operator(&doc_iter, &query_expr)) {
                    return false;
                }
            } else {
                return false; // Invalid query expression
            }
        } else if (bson_iter_type(&filter_iter) == BSON_TYPE_REGEX) {
            // Handle BSON regex type directly
            const char *regex_pattern;
            const char *regex_options;
            
            // Extract regex pattern and options
            regex_pattern = bson_iter_regex(&filter_iter, &regex_options);
            if (!regex_pattern) {
                return false;
            }
            
            // Check if the document field contains a string value
            if (!BSON_ITER_HOLDS_UTF8(&doc_iter)) {
                return false; // Regex only works on string fields
            }
            
            const char *doc_value = bson_iter_utf8(&doc_iter, NULL);
            if (!doc_value) {
                return false;
            }
            
            // Compile and execute regex
            regex_t regex;
            int flags = REG_EXTENDED; // Default flags
            
            // Process MongoDB-style options
            if (regex_options && strlen(regex_options) > 0) {
                for (const char *opt = regex_options; *opt; opt++) {
                    switch (*opt) {
                        case 'i': // Case insensitive
                            flags |= REG_ICASE;
                            break;
                        case 'm': // Multiline
                            flags |= REG_NEWLINE;
                            break;
                        // Other flags not supported in POSIX
                    }
                }
            }
            
            int compile_result = regcomp(&regex, regex_pattern, flags);
            if (compile_result != 0) {
                return false; // Invalid regex pattern
            }
            
            int match_result = regexec(&regex, doc_value, 0, NULL, 0);
            regfree(&regex);
            
            if (match_result != 0) {
                return false; // No match
            }
        } else {
            // Simple equality comparison (backward compatibility)
            if (!bson_iter_values_equal(&filter_iter, &doc_iter)) {
                return false;
            }
        }
    }
    
    return true;  // All filter conditions matched
}

// Helper function to apply projection to document
static bson_t* apply_projection(const bson_t *doc, const bson_t *projection) {
    if (!projection || bson_empty(projection)) {
        return bson_copy(doc);  // No projection, return full document
    }
    
    bson_t *projected_doc = bson_new();
    bson_iter_t proj_iter;
    
    if (!bson_iter_init(&proj_iter, projection)) {
        bson_destroy(projected_doc);
        return bson_copy(doc);  // Invalid projection, return full document
    }
    
    // Always include _id unless explicitly excluded
    bool include_id = true;
    bson_iter_t id_check;
    if (bson_iter_init_find(&id_check, projection, "_id") && 
        bson_iter_as_bool(&id_check) == false) {
        include_id = false;
    }
    
    if (include_id) {
        bson_iter_t doc_id;
        if (bson_iter_init_find(&doc_id, doc, "_id")) {
            bson_append_iter(projected_doc, "_id", -1, &doc_id);
        }
    }
    
    // Include requested fields
    while (bson_iter_next(&proj_iter)) {
        const char *field_name = bson_iter_key(&proj_iter);
        
        if (strcmp(field_name, "_id") == 0) {
            continue;  // Already handled above
        }
        
        if (bson_iter_as_bool(&proj_iter)) {  // Field should be included
            bson_iter_t doc_field;
            if (bson_iter_init_find(&doc_field, doc, field_name)) {
                bson_append_iter(projected_doc, field_name, -1, &doc_field);
            }
        }
    }
    
    return projected_doc;
}

// Create cursor for find operation
mlite_cursor_t* mlite_find(mlite_db_t *db, const char *collection_name,
                          const bson_t *filter, const bson_t *opts) {
    if (!db || !collection_name) {
        return NULL;
    }
    
    // Check if collection exists
    if (!mlite_collection_exists(db, collection_name)) {
        return NULL;
    }
    
    // Allocate cursor structure
    mlite_cursor_t *cursor = malloc(sizeof(mlite_cursor_t));
    if (!cursor) {
        return NULL;
    }
    
    // Initialize cursor
    memset(cursor, 0, sizeof(mlite_cursor_t));
    cursor->db = db;
    cursor->collection_name = strdup(collection_name);
    
    if (filter) {
        cursor->filter = bson_copy(filter);
    }
    
    if (opts) {
        cursor->opts = bson_copy(opts);
    }
    
    // Prepare SQL query to get all documents from collection
    char *sql = NULL;
    int sql_len = snprintf(NULL, 0, "SELECT _id, document FROM collection_%s", collection_name);
    sql = malloc(sql_len + 1);
    if (!sql) {
        if (cursor->filter) bson_destroy(cursor->filter);
        if (cursor->opts) bson_destroy(cursor->opts);
        free(cursor->collection_name);
        free(cursor);
        return NULL;
    }
    snprintf(sql, sql_len + 1, "SELECT _id, document FROM collection_%s", collection_name);
    
    int rc = sqlite3_prepare_v2(db->sqlite_db, sql, -1, &cursor->stmt, NULL);
    free(sql);
    
    if (rc != SQLITE_OK) {
        if (cursor->filter) bson_destroy(cursor->filter);
        if (cursor->opts) bson_destroy(cursor->opts);
        free(cursor->collection_name);
        free(cursor);
        return NULL;
    }
    
    return cursor;
}

// Get next document from cursor
bool mlite_cursor_next(mlite_cursor_t *cursor, const bson_t **doc) {
    if (!cursor || cursor->finished || cursor->has_error) {
        return false;
    }
    
    // Clean up previous document
    if (cursor->current_doc) {
        bson_destroy(cursor->current_doc);
        cursor->current_doc = NULL;
    }
    
    while (true) {
        int rc = sqlite3_step(cursor->stmt);
        
        if (rc == SQLITE_DONE) {
            cursor->finished = true;
            return false;
        }
        
        if (rc != SQLITE_ROW) {
            cursor->has_error = true;
            bson_set_error(&cursor->error, BSON_ERROR_INVALID, 300, 
                          "Database error during cursor iteration: %s", 
                          sqlite3_errmsg(cursor->db->sqlite_db));
            return false;
        }
        
        // Get document BLOB
        const void *blob_data = sqlite3_column_blob(cursor->stmt, 1);
        int blob_size = sqlite3_column_bytes(cursor->stmt, 1);
        
        if (!blob_data || blob_size <= 0) {
            continue;  // Skip invalid documents
        }
        
        // Create BSON document from blob
        bson_t *stored_doc = bson_new();
        if (!bson_init_static(stored_doc, blob_data, blob_size)) {
            bson_destroy(stored_doc);
            continue;  // Skip invalid BSON
        }
        
        // Check if document matches filter
        if (!document_matches_filter(stored_doc, cursor->filter)) {
            bson_destroy(stored_doc);
            continue;  // Document doesn't match filter
        }
        
        // Apply projection if specified
        bson_t *projection = NULL;
        if (cursor->opts) {
            bson_iter_t opts_iter;
            if (bson_iter_init_find(&opts_iter, cursor->opts, "projection") &&
                BSON_ITER_HOLDS_DOCUMENT(&opts_iter)) {
                
                uint32_t len;
                const uint8_t *data;
                bson_iter_document(&opts_iter, &len, &data);
                projection = bson_new_from_data(data, len);
            }
        }
        
        cursor->current_doc = apply_projection(stored_doc, projection);
        
        if (projection) {
            bson_destroy(projection);
        }
        bson_destroy(stored_doc);
        
        *doc = cursor->current_doc;
        return true;
    }
}

// Check if cursor has error
bool mlite_cursor_error(mlite_cursor_t *cursor, bson_error_t *error) {
    if (!cursor) {
        return false;
    }
    
    if (cursor->has_error && error) {
        *error = cursor->error;
    }
    
    return cursor->has_error;
}

// Destroy cursor and free resources
void mlite_cursor_destroy(mlite_cursor_t *cursor) {
    if (!cursor) {
        return;
    }
    
    if (cursor->stmt) {
        sqlite3_finalize(cursor->stmt);
        cursor->stmt = NULL;
    }
    
    if (cursor->filter) {
        bson_destroy(cursor->filter);
        cursor->filter = NULL;
    }
    
    if (cursor->opts) {
        bson_destroy(cursor->opts);
        cursor->opts = NULL;
    }
    
    if (cursor->current_doc) {
        bson_destroy(cursor->current_doc);
        cursor->current_doc = NULL;
    }
    
    if (cursor->collection_name) {
        free(cursor->collection_name);
        cursor->collection_name = NULL;
    }
    
    // Clear the cursor structure to prevent double-free issues
    memset(cursor, 0, sizeof(mlite_cursor_t));
    free(cursor);
}

// Find a single document matching filter
bson_t* mlite_find_one(mlite_db_t *db, const char *collection_name,
                       const bson_t *filter, const bson_t *opts) {
    if (!db || !collection_name) {
        return NULL;
    }
    
    // Use mlite_find to get a cursor
    mlite_cursor_t *cursor = mlite_find(db, collection_name, filter, opts);
    if (!cursor) {
        return NULL;
    }
    
    // Get the first (and only) document
    bson_t *result = NULL;
    const bson_t *doc;
    if (mlite_cursor_next(cursor, &doc)) {
        // Make a copy of the document since cursor will be destroyed
        result = bson_copy(doc);
    }
    
    // Clean up cursor
    mlite_cursor_destroy(cursor);
    
    return result;
}

// Count documents matching filter
int64_t mlite_count_documents(mlite_db_t *db, const char *collection_name,
                             const bson_t *filter, bson_error_t *error) {
    if (!db || !collection_name) {
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 400, "Invalid parameters for count_documents");
        }
        return -1;
    }
    
    // Check if collection exists
    if (!mlite_collection_exists(db, collection_name)) {
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 401, "Collection '%s' does not exist", collection_name);
        }
        return -1;
    }
    
    // For simple implementation, use cursor to count matching documents
    mlite_cursor_t *cursor = mlite_find(db, collection_name, filter, NULL);
    if (!cursor) {
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 402, "Failed to create cursor for counting");
        }
        return -1;
    }
    
    int64_t count = 0;
    const bson_t *doc;
    
    while (mlite_cursor_next(cursor, &doc)) {
        count++;
    }
    
    // Check for cursor errors
    if (mlite_cursor_error(cursor, error)) {
        mlite_cursor_destroy(cursor);
        return -1;
    }
    
    mlite_cursor_destroy(cursor);
    return count;
}