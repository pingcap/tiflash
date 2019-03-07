#pragma once

template<typename Type>
inline bool isNaN(Type tp) {
    return tp != tp;
}

template<typename Type>
inline bool isInf(Type tp) {
    return !isNaN(tp) && isNaN(tp - tp);
}

