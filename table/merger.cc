// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {
class MergingIterator : public Iterator
{
public:
    MergingIterator(const Comparator* comparator, Iterator** children, int n)
        : comparator_(comparator)
        , children_(new IteratorWrapper[n])
        , n_(n)
        , current_(nullptr)
        , direction_(kForward)
    {
        for (int i = 0; i < n; i++) {
            children_[i].Set(children[i]);
        }
    }

    ~MergingIterator() override { delete[] children_; }

    bool Valid() const override { return (current_ != nullptr); }

    void SeekToFirst() override
    {
        for (int i = 0; i < n_; i++) {
            children_[i].SeekToFirst();
        }
        FindSmallest();
        direction_ = kForward;
    }

    void SeekToLast() override
    {
        for (int i = 0; i < n_; i++) {
            children_[i].SeekToLast();
        }
        FindLargest();
        direction_ = kReverse;
    }

    void Seek(const Slice& target) override
    {
        for (int i = 0; i < n_; i++) {
            children_[i].Seek(target);
        }
        FindSmallest();
        direction_ = kForward;
    }

  void Next() override {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kForward) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
        }
      }
      direction_ = kForward;
    }

    current_->Next();
    FindSmallest();
  }

  void Prev() override {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse;
    }

    current_->Prev();
    FindLargest();
  }

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  Status status() const override {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

private:
    // Which direction is the iterator moving?
    enum Direction { kForward, kReverse };

    void FindSmallest();
    void FindLargest();

    const Comparator* comparator_;
    // 当存在许多 children 时可能是用一个堆更合适
    // 这里考虑到 leveldb 的 children 可能不会很多, 因此简单的使用了一个 array
    IteratorWrapper* children_;
    int n_;
    IteratorWrapper* current_;
    Direction direction_;
};

/**
 * 遍历所有的 children iterator
 * 寻找当前 entry 的 key 最小的 iterator 并 set 到 current_
 */
void MergingIterator::FindSmallest()
{
    IteratorWrapper* smallest = nullptr;
    for (int i = 0; i < n_; i++)
    {
        IteratorWrapper* child = &children_[i];
        if (child->Valid())
        {
            if (smallest == nullptr)
            {
                smallest = child;
            }
            else if (comparator_->Compare(child->key(), smallest->key()) < 0)
            {
                smallest = child;
            }
        }
    }
    current_ = smallest;
}

/**
 * 遍历所有的 children iterator
 * 寻找当前 entry 的 key 最大的 iterator 并 set 到 current_
 */
void MergingIterator::FindLargest()
{
    IteratorWrapper* largest = nullptr;
    for (int i = n_ - 1; i >= 0; i--)
    {
        IteratorWrapper* child = &children_[i];
        if (child->Valid())
        {
            if (largest == nullptr)
            {
                largest = child;
            }
            else if (comparator_->Compare(child->key(), largest->key()) > 0)
            {
                largest = child;
            }
        }
    }
    current_ = largest;
}
}  // namespace

/**
 * 返回一个 iterator, 该 iterator 提供 children[0, n-1] 中数据的 union
 * 这个 iterator 拥有子 iters 的所有权, 并在其自身被析构时会析构子 iterator
 * 
 * 所生成的 iterator 不会负责对数据做去重, 即如果一个 key 出现在 K 个子 iters 中, 则会得到其 K 次
 * @param comparator[IN]
 * @param children[IN]
 * @param n[IN]
 * @return 返回一个聚合的 Iterator
 */
Iterator*
NewMergingIterator(const Comparator* comparator,
                   Iterator** children,
                   int n)
{
    assert(n >= 0);

    if (n == 0)
        return NewEmptyIterator();
    else if (n == 1)
        return children[0];
    else
        return new MergingIterator(comparator, children, n);
}

}  // namespace leveldb
