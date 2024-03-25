package io.airbyte.commons.enums

import com.google.common.base.Preconditions
import com.google.common.collect.Maps
import com.google.common.collect.Sets
import java.util.*
import java.util.concurrent.ConcurrentMap
import java.util.function.Function
import java.util.stream.Collectors

class Enums {
    companion object {
        inline fun <T1 : Enum<T1>, reified T2 : Enum<T2>> convertTo(ie: T1?, oe: Class<T2>): T2? {
            if (ie == null) {
                return null
            }

            return enumValueOf<T2>(ie.name)
        }

        private fun normalizeName(name: String): String {
            return name.lowercase(Locale.getDefault()).replace("[^a-zA-Z0-9]".toRegex(), "")
        }

        fun <T1 : Enum<T1>?> valuesAsStrings(e: Class<T1>): Set<String> {
            Preconditions.checkArgument(e.isEnum)
            return Arrays.stream(e.enumConstants)
                .map { obj: T1 -> obj!!.name }
                .collect(Collectors.toSet())
        }

        fun <T : Enum<T>> toEnum(value: String, enumClass: Class<T>): Optional<T> {
            Preconditions.checkArgument(enumClass.isEnum)

            if (!NORMALIZED_ENUMS.containsKey(enumClass)) {
                val values = enumClass.enumConstants
                val mappings: MutableMap<String, T> = Maps.newHashMapWithExpectedSize(values.size)
                for (t in values) {
                    mappings[normalizeName(t!!.name)] = t
                }
                NORMALIZED_ENUMS.put(enumClass, mappings)
            }

            return Optional.ofNullable<T>(
                NORMALIZED_ENUMS.getValue(enumClass).getValue(normalizeName(value)) as T
            )
        }

        private val NORMALIZED_ENUMS: ConcurrentMap<Class<*>, Map<String, *>> =
            Maps.newConcurrentMap()

        fun <T1 : Enum<T1>?, T2 : Enum<T2>?> isCompatible(c1: Class<T1>, c2: Class<T2>): Boolean {
            Preconditions.checkArgument(c1.isEnum)
            Preconditions.checkArgument(c2.isEnum)
            return (c1.enumConstants.size == c2.enumConstants.size &&
                Sets.difference(
                        Arrays.stream(c1.enumConstants)
                            .map { obj: T1 -> obj!!.name }
                            .collect(Collectors.toSet()),
                        Arrays.stream(c2.enumConstants)
                            .map { obj: T2 -> obj!!.name }
                            .collect(Collectors.toSet())
                    )
                    .isEmpty())
        }

        inline fun <T1 : Enum<T1>, reified T2 : Enum<T2>> convertListTo(
            ies: List<T1>,
            oe: Class<T2>
        ): List<T2> {
            return ies.stream()
                .map<T2>(Function<T1, T2> { ie: T1 -> Enums.convertTo<T1, T2>(ie, oe) })
                .collect(Collectors.toList<T2>())
        }
    }
}
