import java.nio.file.Paths

class DockerHelpers {
    static String extractLabelValue(String dockerFile, String labelName) {
        def file = dockerFile instanceof File ? dockerFile : new File(dockerFile)
        return file.readLines()
                .grep({ it.startsWith('LABEL') && it.contains(labelName) })
                .get(0)
                .split('=')[1]
    }

    static String extractImageName(String dockerFile) {
        return extractLabelValue(dockerFile, "io.airbyte.name")
    }

    static String extractImageVersion(String dockerFile) {
        return extractLabelValue(dockerFile, "io.airbyte.version")
    }

    static String getDockerfilePath(projectDir, String dockerfileName) {
        return Paths.get(projectDir.absolutePath, dockerfileName)
    }

    static String getDevTaggedImage(projectDir, dockerfileName) {
        return "${extractImageName(getDockerfilePath(projectDir, dockerfileName).toString())}:dev"
    }
}
