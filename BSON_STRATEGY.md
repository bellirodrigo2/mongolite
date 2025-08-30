# MongoLite BSON Strategy Decision

**Date**: August 30, 2025  
**Status**: ✅ IMPLEMENTED - Header-only interface with dual backend support

## Decision Summary

**Chosen Approach**: Use battle-tested libbson with abstraction layer for future flexibility

### Key Decision Points

1. **Battle-tested vs Custom**
   - ✅ **libbson**: Proven in production, zero BSON bugs, maintained by MongoDB
   - ❌ **Custom**: Risky for database (data corruption, edge cases, testing burden)

2. **Size vs Reliability Trade-off**
   - **libbson**: ~50-150KB estimated (acceptable for embedded)
   - **Custom**: ~30KB but unproven
   - **Conclusion**: For a database, correctness > size optimization

3. **Future Flexibility**
   - Clean interface layer allows drop-in replacement later
   - Can optimize/strip libbson features if size becomes critical
   - Immediate compatibility with MongoDB ecosystem

## Repository Information

- **Source**: `https://github.com/mongodb/mongo-c-driver`
- **libbson location**: `/src/libbson/`
- **Current version**: 2.0.2+ (2025)
- **Standalone build**: `cmake -DENABLE_MONGOC=OFF`

## Implementation Strategy

### Phase 2A: libbson Integration (1-2 weeks)
```c
// mongolite/bson_interface.h - Clean abstraction
typedef struct mongolite_bson mongolite_bson_t;

mongolite_bson_t* mongolite_bson_from_json(const char* json);
char* mongolite_bson_to_json(const mongolite_bson_t* bson);
const uint8_t* mongolite_bson_get_data(const mongolite_bson_t* bson, uint32_t* length);
void mongolite_bson_destroy(mongolite_bson_t* bson);
```

### Phase 2B: SQLite Integration
- Store BSON as SQLite BLOBs
- Use SQLite JSON functions for queries
- Implement collection management

## Implementation Completed ✅

### What Was Built
1. ✅ **Header-Only Interface**: `mongolite_bson.h` with zero-cost inline wrappers
2. ✅ **Standalone Implementation**: `mongolite_standalone.h/.c` for immediate testing  
3. ✅ **Clean Abstraction**: User chose "option 5" header-only approach
4. ✅ **SQLite Integration**: BSON documents stored as BLOBs, working end-to-end

### Current Implementation
```c
// mongolite_bson.h - Zero-cost abstraction over libbson
typedef bson_t mongolite_bson_t;
static inline mongolite_bson_t* mongolite_bson_new(void) {
    return bson_new();
}
```

### Architecture Achieved
- **Development**: Standalone BSON implementation (no dependencies)
- **Production Ready**: Header-only libbson integration (when libbson available)  
- **User's Choice**: Easy replacement "not a priority, since it could never happen"

## Next Phase Options

1. **Production Deployment**: Build with full libbson for production use
2. **Keep Standalone**: Current implementation works for embedded/simple cases
3. **Hybrid Approach**: Configure at build time based on requirements

## Strategic Benefits

✅ **Immediate**: Battle-tested BSON handling, MongoDB compatibility  
✅ **Future**: Can optimize/replace via interface if needed  
✅ **Risk**: Minimal - proven solution with escape path