/* Copyright (c) 2020, Stanford University
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef HOMA_OBJECTALLOCATOR_H
#define HOMA_OBJECTALLOCATOR_H

namespace Homa {

/**
 * Represents a memory allocator that allocates memory in fixed-size chunks,
 * or objects.
 *
 * This class is thread-safe.
 */
class ObjectAllocator {
  public:
    /**
     * Constructor.
     *
     * @param objectSize
     *      Size of the object to allocate from this allocator.
     */
    explicit ObjectAllocator(size_t objectSize)
        : objectSize(objectSize)
    {}

    /**
     * Destructor.
     */
    virtual ~ObjectAllocator() = default;

    /**
     * Allocate a fixed-size chunk of memory.
     *
     * \return
     *      A pointer to the new object.
     */
    virtual void* allocate() = 0;

    /**
     * Return an object previously allocated by this allocator.
     *
     * \param object
     *      A pointer to object to be released.
     */
    virtual void release(void* object) = 0;

    /**
     * Return the number of objects for which allocate() was called, but
     * release() was not.  Primarily intended for testing.
     */
    uint64_t numOutstandingObjects()
    {
        return outstandingObjects;
    }

  protected:
    /// Size of the memory to allocate for each object, in bytes.
    const size_t objectSize;

    /// Count of the number of objects for which allocate() was called, but
    /// release() was not.
    uint64_t outstandingObjects;
};


/**
 * A simple implementation of the ObjectAllocator API.
 *
 * For simplicity, this implementation uses a straightforward monitor-style lock
 * to ensure thread-safety.
 */
class SimpleObjectAllocator : public ObjectAllocator {
  public:
    explicit SimpleObjectAllocator(size_t objectSize)
        : ObjectAllocator(objectSize)
        , mutex()
        , objects()
    {}

    ~SimpleObjectAllocator() override
    {
        for (void* object : objects) {
            std::free(object);
        }
    }

    void* allocate() override
    {
        SpinLock::Lock _(mutex);
        outstandingObjects++;
        if (objects.empty()) {
            return std::malloc(objectSize);
        }
        void* object = objects.back();
        objects.pop_back();
        return object;
    }

    void release(void* object) override
    {
        SpinLock::Lock _(mutex);
        outstandingObjects--;
        objects.push_back(object);
    }

  private:
    /// Monitor-style lock.
    SpinLock mutex;

    /// List of objects previously released.
    std::vector<void*> objects;
};

}  // namespace Homa

#endif  //HOMA_OBJECTALLOCATOR_H
