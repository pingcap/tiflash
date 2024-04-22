// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/NotNull.h>
#include <common/defines.h>
#include <gtest/gtest.h>

namespace DB::tests
{

struct Ball
{
    struct Asserts
    {
        size_t destructed_count = 0;
    };

    Ball(Asserts * asserts_)
    {
        copied = 0;
        moved = 0;
        value = 0;
        copy_assigned = 0;
        move_assigned = 0;
        asserts = asserts_;
    }

    Ball()
        : Ball(nullptr)
    {}

    Ball(const Ball & ball)
        : copied(ball.copied + 1)
        , moved(ball.moved)
        , copy_assigned(ball.copy_assigned)
        , move_assigned(ball.move_assigned)
        , value(ball.value)
    {}

    Ball(Ball && ball)
        : copied(ball.copied)
        , moved(ball.moved + 1)
        , copy_assigned(ball.copy_assigned)
        , move_assigned(ball.move_assigned)
        , value(ball.value)
    {}

    Ball & operator=(const Ball & p)
    {
        copied = p.copied;
        moved = p.moved;
        copy_assigned = p.copy_assigned + 1;
        move_assigned = p.move_assigned;
        value = p.value;
        return *this;
    }

    Ball & operator=(Ball && p)
    {
        copied = p.copied;
        moved = p.moved;
        copy_assigned = p.copy_assigned;
        move_assigned = p.move_assigned + 1;
        value = p.value;
        return *this;
    }

    ~Ball()
    {
        if (asserts != nullptr)
        {
            asserts->destructed_count++;
        }
        return;
    }

    size_t copied;
    size_t moved;
    size_t copy_assigned;
    size_t move_assigned;
    int value;
    Asserts * asserts;
};

template <typename T>
struct MockSharedPtr
{
    using element_type = T;
    using pointer = element_type *;
    using reference = typename std::add_lvalue_reference<element_type>::type;

    MockSharedPtr(T * ptr)
        : hold_ptr(ptr)
    {
        copied = 0;
        moved = 0;
    }

    MockSharedPtr(const MockSharedPtr & p)
    {
        hold_ptr = p.hold_ptr;
        copied = p.copied + 1;
        moved = p.moved;
    }

    MockSharedPtr(MockSharedPtr && p)
    {
        hold_ptr = p.hold_ptr;
        copied = p.copied;
        moved = p.moved + 1;
    }

    T & operator=(const MockSharedPtr & p) { hold_ptr = p.hold_ptr; }
    T & operator=(MockSharedPtr && p) { hold_ptr = p.hold_ptr; }

    T & operator*() const { return *hold_ptr; }
    T * operator->() const { return hold_ptr; }

    operator bool() const { return hold_ptr == nullptr; }

    size_t get_copied() const { return copied; }
    size_t get_moved() const { return moved; }

    T * get() const noexcept { return hold_ptr; }

private:
    T * hold_ptr;
    size_t copied;
    size_t moved;
};

template <class T>
bool operator==(const MockSharedPtr<T> & lhs, std::nullptr_t) noexcept
{
    if (lhs)
    {
        return true;
    }
    return false;
}

template <typename T>
void mustNotNull(const NotNull<T> & nn)
{
    RUNTIME_CHECK(nn != nullptr);
}

template <typename T>
void mustNotNullPtr(const NotNullShared<T> & nn)
{
    RUNTIME_CHECK(nn != nullptr);
}

template <typename T>
void mustNotNullUPtr(const NotNullUnique<T> & nn)
{
    RUNTIME_CHECK(nn != nullptr);
}

// Use volatile to prevent optimization.
static volatile int MAYBE_NOT_ZERO = 0;

template <typename T>
T * getNullPtr()
{
    return reinterpret_cast<T *>(MAYBE_NOT_ZERO);
}

template <typename T, typename U>
void must_not_assign(const T &, const U &)
{
    static_assert(!std::is_assignable_v<T, U>);
}

template <typename T, typename U>
void must_not_construct(const U &)
{
    static_assert(!std::is_constructible_v<T, U>);
}

template <typename T>
void must_not_copy(const T &)
{
    static_assert(!std::is_copy_constructible_v<T>);
}

TEST(NotNullTest, Raw)
{
    [[maybe_unused]] auto p1 = newNotNull(new int(1));
    must_not_assign(p1, nullptr);
    EXPECT_ANY_THROW(p1 = newNotNull(getNullPtr<int>()));
}

TEST(NotNullTest, Shared)
{
    NotNull<std::shared_ptr<Ball>> p1 = makeNotNullShared<Ball>();
    auto p2 = std::move(p1);
    ASSERT_EQ(p2->moved, 0);
    ASSERT_EQ(p2->copied, 0);
    NotNull<std::shared_ptr<Ball>> p3(p2);
    ASSERT_EQ(p3->moved, 0);
    ASSERT_EQ(p3->copied, 0);
    p2->value = 1;
    ASSERT_EQ(p3->value, 1);
    must_not_assign(p1, nullptr);
    must_not_assign(p1, std::make_shared<Ball>(nullptr));
}

TEST(NotNullTest, MockShared)
{
    Ball * ball = new Ball();
    auto p1 = newNotNull<MockSharedPtr<Ball>>(MockSharedPtr(ball));
    auto base_move = p1.as_nullable().get_moved();
    auto p2 = std::move(p1);
    ASSERT_EQ(p2.as_nullable().get_moved(), base_move + 1);
    ASSERT_EQ(p2.as_nullable().get_copied(), 0);
    auto p3(p2);
    ASSERT_EQ(p3.as_nullable().get_moved(), base_move + 1);
    ASSERT_EQ(p3.as_nullable().get_copied(), 1);
    p2->value = 1;
    ASSERT_EQ(p3->value, 1);
    must_not_assign(p1, nullptr);
}


TEST(NotNullTest, Unique)
{
    auto p1 = makeNotNullUnique<Ball>();
    auto p2 = std::move(p1);
    mustNotNullUPtr(p2);
    ASSERT_EQ(p2->moved, 0);
    ASSERT_EQ(p2->copied, 0);
}

namespace
{
void takesUnique(std::unique_ptr<Ball> ptr)
{
    // Either move of not_null or unqiue_ptr will not change `moved`.
    ASSERT_EQ(ptr->moved, 0);
}

void takesShared(std::shared_ptr<Ball> ptr)
{
    ASSERT_EQ(ptr->moved, 0);
}

void takesNotNullUnique(NotNullUnique<Ball> ptr)
{
    takesUnique(std::move(ptr).as_nullable());
}

void takesNotNullShared(NotNullShared<Ball> ptr)
{
    takesShared(ptr.as_nullable());
}
} // namespace

TEST(NotNullTest, ToNullablePointer)
{
    Ball::Asserts asserts;
    auto p1 = makeNotNullUnique<Ball>(&asserts);
    takesNotNullUnique(std::move(p1));
    ASSERT_EQ(asserts.destructed_count, 1);
    // Can't copy-constructs not_null<unique_ptr<T>>
    auto p3 = makeNotNullUnique<Ball>(&asserts);
    must_not_copy(p3);

    auto p2 = makeNotNullShared<Ball>(&asserts);
    takesNotNullShared(p2);
    ASSERT_EQ(asserts.destructed_count, 1); // destructs when return
    auto p2_1 = makeNotNullShared<Ball>(&asserts);
    takesNotNullShared(std::move(p2_1));
    ASSERT_EQ(asserts.destructed_count, 2); // destructs when return
    auto p2_2 = makeNotNullShared<Ball>(&asserts);
    auto p2_2_1 = p2_2.as_nullable();
    takesNotNullShared(std::move(p2_2));
    ASSERT_EQ(asserts.destructed_count, 2);
}

namespace
{
void consNotNullPointer(std::unique_ptr<Ball> ptr)
{
    auto p = newNotNull(std::move(ptr));
    ASSERT_EQ(p->copied, 0);
    ASSERT_EQ(p->moved, 0);
}
void consNotNullPointer(std::shared_ptr<Ball> ptr)
{
    auto p = newNotNull(std::move(ptr));
    ASSERT_EQ(p->copied, 0);
    ASSERT_EQ(p->moved, 0);
}
} // namespace

TEST(NotNullTest, ToNotNullPointer)
{
    Ball::Asserts asserts;
    auto p1 = std::make_unique<Ball>(&asserts);
    consNotNullPointer(std::move(p1));
    ASSERT_EQ(asserts.destructed_count, 1);
    auto p2 = std::make_shared<Ball>(&asserts);
    consNotNullPointer(std::move(p2));
    ASSERT_EQ(asserts.destructed_count, 2);
    auto p2_1 = std::make_shared<Ball>(&asserts);
    auto p2_1_1 = p2_1;
    consNotNullPointer(std::move(p2_1));
    ASSERT_EQ(asserts.destructed_count, 2);
}

namespace
{
template <typename T, typename U>
void convertNotNullPointer(NotNull<U> ptr)
{
    NotNull<T> p = cpp::bitwizeshift::not_null<T>(std::move(ptr));
    ASSERT_EQ(p->copied, 0);
    ASSERT_EQ(p->moved, 0);
}
} // namespace

TEST(NotNullTest, Converts)
{
    Ball::Asserts asserts;
    auto p1 = makeNotNullUnique<Ball>(&asserts);
    convertNotNullPointer<std::shared_ptr<Ball>>(std::move(p1));
    ASSERT_EQ(asserts.destructed_count, 1);

    // auto p2 = makeNotNullShared<Ball>(&asserts);
    // convertNotNullPointer<std::unique_ptr<Ball>>(std::move(p2));

    static_assert(NOT_NULL_NS_IMPL::detail::
                      not_null_is_implicit_convertible<std::shared_ptr<Ball>, std::unique_ptr<Ball> &&>::value);
    static_assert(!NOT_NULL_NS_IMPL::detail::
                      not_null_is_implicit_convertible<std::unique_ptr<Ball>, std::shared_ptr<Ball> &&>::value);
    static_assert(!NOT_NULL_NS_IMPL::detail::
                      not_null_is_explicit_convertible<std::shared_ptr<Ball>, std::unique_ptr<Ball> &&>::value);
    static_assert(!NOT_NULL_NS_IMPL::detail::
                      not_null_is_explicit_convertible<std::unique_ptr<Ball>, std::shared_ptr<Ball> &&>::value);
}

} // namespace DB::tests