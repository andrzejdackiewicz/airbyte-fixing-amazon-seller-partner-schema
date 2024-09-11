package io.airbyte.cdk.test.util

interface NameMangler {
    fun mangleStream(namespace: String?, name: String): Pair<String?, String>
    fun mangleFieldName(path: List<String>): List<String>
}

class NoopNameMangler : NameMangler {
    override fun mangleStream(namespace: String?, name: String): Pair<String?, String> =
        Pair(namespace, name)

    override fun mangleFieldName(path: List<String>): List<String> = path
}
