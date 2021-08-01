#include <Functions/DispatchHelper.h>
#include <gtest/gtest.h>

namespace DB
{

namespace tests
{

namespace helper = DispatchHelper;

class TestDispatchHelper : public ::testing::Test
{
protected:
    struct SumOfSizeChecker
    {
        template <typename T>
        static constexpr bool check()
        {
            return !std::is_same_v<T, long long>;
        }

        template <typename T, typename, typename, typename U>
        static constexpr bool check()
        {
            if constexpr (sizeof(T) >= sizeof(int))
                return U();
            else
                return true;
        }
    };

    template <typename... Ts>
    struct SumOfSize
    {
        static size_t apply(size_t scale) { return scale * (sizeof(Ts) + ...); }
    };
};

TEST_F(TestDispatchHelper, TypeVar)
{
    auto typevar = helper::impl::TypeVar<int>();
    ASSERT_TRUE((std::is_same_v<helper::unwrap_t<decltype(typevar)>, int>));

    helper::impl::TypeVarPtr ptr = helper::getTypeVar<int>();
    ASSERT_NE(dynamic_cast<const helper::impl::TypeVar<int> *>(ptr), nullptr);
    ASSERT_EQ(ptr->getTypeIndex(), std::type_index(typeid(int)));
    ASSERT_EQ(ptr->getName(), typeid(int).name());
}

TEST_F(TestDispatchHelper, Dispatcher)
{
    auto table = helper::newBuilder()
                     .append<short, int, long long>()
                     .append<signed char, unsigned char>()
                     .append<float, double, long double>()
                     .append<std::true_type, std::false_type>()
                     .setSignature<void ()>()
                     .setSignature<int (int, short, char, float)>()
                     .setSignature<size_t(size_t)>()
                     .buildFor<SumOfSize>();

    constexpr size_t sizes[][3] = {
        {sizeof(short), sizeof(int), sizeof(long long)},
        {sizeof(signed char), sizeof(unsigned char)},
        {sizeof(float), sizeof(double), sizeof(long double)},
        {sizeof(std::true_type), sizeof(std::false_type)}};

    for (size_t a = 0; a < 3; ++a)
    {
        for (size_t b = 0; b < 2; ++b)
        {
            for (size_t c = 0; c < 3; ++c)
            {
                for (size_t d = 0; d < 2; ++d)
                {
                    auto fn = table.lookup(a, b, c, d);
                    ASSERT_NE(fn, nullptr);

                    size_t expected = sizes[0][a] + sizes[1][b] + sizes[2][c] + sizes[3][d];
                    ASSERT_EQ(fn(1), expected);
                    ASSERT_EQ(fn(2), 2 * expected);
                    ASSERT_EQ(fn(3), 3 * expected);
                }
            }
        }
    }
}

TEST_F(TestDispatchHelper, Checker)
{
    auto table = helper::newBuilder()
                     .append<short, int, long long>()
                     .append<signed char, unsigned char>()
                     .append<float, double, long double>()
                     .append<std::true_type, std::false_type>()
                     .setSignature<size_t(size_t)>()
                     .buildFor<SumOfSize, SumOfSizeChecker>();

    ASSERT_EQ(table.lookup(1, 0, 0, 0), (&SumOfSize<int, signed char, float, std::true_type>::apply));
    ASSERT_EQ(table.lookup(2, 0, 0, 0), nullptr);
    ASSERT_EQ(table.lookup({1, 0, 0, 0}), (&SumOfSize<int, signed char, float, std::true_type>::apply));
    ASSERT_EQ(table.lookup({1, 0, 0, 1}), nullptr);
    ASSERT_EQ(table.lookup({
        helper::getTypeVar<short>(), helper::getTypeVar<unsigned char>(), helper::getTypeVar<double>(),
        helper::getTypeVar<std::bool_constant<true>>()}),
        (&SumOfSize<short, unsigned char, double, std::true_type>::apply));
    ASSERT_EQ(table.lookup({
        helper::getTypeVar<short>(), helper::getTypeVar<unsigned char>(), helper::getTypeVar<double>(),
        helper::getTypeVar<std::bool_constant<false>>()}),
        (&SumOfSize<short, unsigned char, double, std::false_type>::apply));
    ASSERT_EQ(table.lookup({
        helper::getTypeVar<int>(), helper::getTypeVar<unsigned char>(), helper::getTypeVar<double>(),
        helper::getTypeVar<std::bool_constant<true>>()}),
        (&SumOfSize<int, unsigned char, double, std::true_type>::apply));
    ASSERT_EQ(table.lookup({
        helper::getTypeVar<int>(), helper::getTypeVar<char>(), helper::getTypeVar<double>(),
        helper::getTypeVar<std::bool_constant<true>>()}),
        nullptr);
    ASSERT_EQ(table.lookup({
        helper::getTypeVar<int>(), helper::getTypeVar<unsigned char>(), helper::getTypeVar<double>(),
        helper::getTypeVar<std::bool_constant<false>>()}),
        nullptr);
    ASSERT_EQ(table.lookup({
        helper::getTypeVar<long long>(), helper::getTypeVar<unsigned char>(), helper::getTypeVar<double>(),
        helper::getTypeVar<std::bool_constant<true>>()}),
        nullptr);
    ASSERT_EQ(table.lookup({
        helper::getTypeVar<long long>(), helper::getTypeVar<unsigned char>(), helper::getTypeVar<double>(),
        helper::getTypeVar<std::bool_constant<false>>()}),
        nullptr);
}

TEST_F(TestDispatchHelper, Selector)
{
    auto result = helper::selectFrom<char, short, int, float>()
                      .where([](auto typevar) { return std::is_floating_point_v<helper::unwrap_t<decltype(typevar)>>; })
                      .toTypeVar();
    ASSERT_EQ(result->getTypeIndex(), std::type_index(typeid(float)));

    bool failed = false;
    result = helper::selectFrom<int, float>().where([](auto) { return false; }).onError([&] { failed = true; }).toTypeVar();
    ASSERT_TRUE(failed);

    size_t int_sizes[] = {1, 2, 4};
    std::type_index indexes[] = {typeid(char), typeid(short), typeid(int)};

    for (size_t i = 0; i < 3; ++i)
    {
        size_t int_size = int_sizes[i];
        auto result = helper::selectFrom<int *, char *, short *>()
                          .where([&](auto v) { return sizeof(std::remove_pointer_t<helper::unwrap_t<decltype(v)>>) == int_size; })
                          .onError([] { std::cerr << "???" << std::endl; })
                          .transform<std::remove_pointer>()
                          .toTypeVar();
        ASSERT_EQ(result->getTypeIndex(), indexes[i]);
    }
}

TEST_F(TestDispatchHelper, OneLine)
{
    auto result = helper::newBuilder()
                      .append<char, short>()
                      .append<int, double, long double>()
                      .setSignature<size_t(size_t)>()
                      .buildFor<SumOfSize>()
                      .lookup({helper::getTypeVar<char>(), helper::getTypeVar<double>()})(3);
    ASSERT_TRUE((std::is_same_v<decltype(result), size_t>));
    ASSERT_EQ(result, 3 * (sizeof(char) + sizeof(double)));
}

TEST_F(TestDispatchHelper, GetTypeVector)
{
    auto table = helper::newBuilder()
                      .append<char, short>()
                      .append<int, double, long double>()
                      .setSignature<size_t(size_t)>()
                      .buildFor<SumOfSize>();

    auto index = table.createTypeVector();
    index[0] = helper::getTypeVar<short>();
    index[1] = helper::getTypeVar<int>();

    auto fn = table.lookup(index);
    ASSERT_NE(fn, nullptr);

    auto result = fn(5);
    ASSERT_TRUE((std::is_same_v<decltype(result), size_t>));
    ASSERT_EQ(result, 5 * (sizeof(short) + sizeof(int)));
}

} // namespace tests

} // namespace DB
