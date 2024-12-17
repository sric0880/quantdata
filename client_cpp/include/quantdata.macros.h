#pragma once
#if defined(_MSC_VER)
#define QD_FUNC __FUNCTION__
#else
#define QD_FUNC __func__
#endif

#if defined(__GNUC__)
#define QD_LIKELY(x) __builtin_expect(!!(x), 1)
#define QD_UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#define QD_LIKELY(v) v
#define QD_UNLIKELY(v) v
#endif

/* Used for asserting parameters to provide a more precise error message */
#define QD_ASSERT_PARAM(param)                                          \
   do                                                                   \
   {                                                                    \
      if ((QD_UNLIKELY(param == NULL)))                                 \
      {                                                                 \
         fprintf(stderr,                                                \
                 "The parameter: %s, in function %s, cannot be NULL\n", \
                 #param,                                                \
                 QD_FUNC);                                              \
         abort();                                                       \
      }                                                                 \
   } while (0)

#define QD_ASSERT_STRING(param)                                          \
   do                                                                    \
   {                                                                     \
      if ((QD_UNLIKELY(param == NULL || !param[0])))                     \
      {                                                                  \
         fprintf(stderr,                                                 \
                 "The parameter: %s, in function %s, cannot be EMPTY\n", \
                 #param,                                                 \
                 QD_FUNC);                                               \
         abort();                                                        \
      }                                                                  \
   } while (0)

/**
 * @brief Assert that the given pointer is non-NULL, while also evaluating to
 * that pointer.
 *
 * Can be used to inline assertions with a pointer dereference:
 *
 * ```
 * foo* f = get_foo();
 * bar* b = BSON_ASSERT_PTR_INLINE(f)->bar_value;
 * ```
 */
#define QD_ASSERT_PTR_INLINE(Pointer) \
   QD_ASSERT_INLINE((Pointer) != NULL, (Pointer))

#define QD_ASSERT(test, err_msg)                              \
   do                                                         \
   {                                                          \
      if (!(QD_LIKELY(test)))                                 \
      {                                                       \
         fprintf(stderr,                                      \
                 "%s:%d %s(): precondition failed: %s, %s\n", \
                 __FILE__,                                    \
                 __LINE__,                                    \
                 QD_FUNC,                                     \
                 #test,                                       \
                 err_msg);                                    \
         abort();                                             \
      }                                                       \
   } while (0)

/**
 * @brief Assert the expression `Assertion`, and evaluates to `Value` on
 * success.
 */
#define QD_ASSERT_INLINE(Assertion, Value)                              \
   ((void)((Assertion) ? (0)                                            \
                       : ((fprintf(stderr,                              \
                                   "%s:%d %s(): Assertion '%s' failed", \
                                   __FILE__,                            \
                                   __LINE__,                            \
                                   QD_FUNC,                             \
                                   #Assertion),                         \
                           abort()),                                    \
                          0)),                                          \
    Value)
