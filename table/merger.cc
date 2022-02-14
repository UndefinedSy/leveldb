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

    // check 当前 current 所指向的 iter 是否有效
    bool Valid() const override { return (current_ != nullptr); }

    // 先将每个 iter 置到 head
    // 然后找所有 iter 中最小的置为 current
    void SeekToFirst() override
    {
        for (int i = 0; i < n_; i++) {
            children_[i].SeekToFirst();
        }
        FindSmallest();
        direction_ = kForward;
    }

    // 先将每个 iter 置到 tail
    // 然后找所有 iter 中最大的置为 current
    void SeekToLast() override
    {
        for (int i = 0; i < n_; i++) {
            children_[i].SeekToLast();
        }
        FindLargest();
        direction_ = kReverse;
    }

    // 先将每个 iter 定位到 >= target 的第一个位置
    // 然后找所有 iter 中最小的置为 current
    void Seek(const Slice& target) override
    {
        for (int i = 0; i < n_; i++) {
            children_[i].Seek(target);
        }
        FindSmallest();
        direction_ = kForward;
    }

    // 将所有子 iter 都移动到 current_->key() 之后的位置
    // 然后将所有 iter 中最小的一个置为 current_
    void Next() override
    {
        assert(Valid());

        // 确保所有的子 iters 都在 key() 之后的位置
        // - 如果当前是 kForward 的方向, 
        //   那么对于所有 non-current_ 的子 iter 来说上述前提成立,
        //   因为 current_ 是当前最小的 iter, 且 key() == current_->key()
        // - 如果当前是 kReverse 的方向,
        //   则我们需要显式地去定位那些 non-current_ 的子 iters
        //   并将其方向设置为 kForward
        if (direction_ != kForward)
        {
            for (int i = 0; i < n_; i++)
            {
                IteratorWrapper* child = &children_[i];
                if (child != current_)
                {
                    child->Seek(key());
                    if (child->Valid()
                        && comparator_->Compare(key(), child->key()) == 0)
                    {
                        child->Next();
                    }
                }
            }
            direction_ = kForward;
        }

        current_->Next();
        FindSmallest();
    }

    void Prev() override
    {
        assert(Valid());

        // 确保所有的子 iters 都在 key() 之前的位置
        // - 如果当前是 kReverse 的方向, 
        //   那么对于所有 non-current_ 的子 iter 来说上述前提成立,
        //   因为 current_ 是当前最大的 iter, 且 key() == current_->key()
        // - 如果当前是 kForward 的方向,
        //   则我们需要显式地去定位那些 non-current_ 的子 iters
        //   并将方向设置为 kReverse
        if (direction_ != kReverse)
        {
            for (int i = 0; i < n_; i++)
            {
                IteratorWrapper* child = &children_[i];
                if (child != current_) {
                    child->Seek(key());
                    if (child->Valid())
                    {
                        // 当前 iter 位于第一个 >= key() 的位置
                        // 进行一个 Prev() 使得回退到 < key()
                        child->Prev();
                    }
                    else
                    {
                        // 当前 iter 中没有 >= key() 的数据
                        // 置到 last entry
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

    // check 所有子 iter 的状态, 如果有一个处于 non-OK 则返回
    Status status() const override
    {
        Status status;
        for (int i = 0; i < n_; i++)
        {
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
 * 寻找当前 entry 的 user key 最小的 iterator 并 set 到 current_
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
 * 寻找当前 entry 的 user key 最大的 iterator 并 set 到 current_
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
