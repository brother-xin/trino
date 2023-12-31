package io.trino.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.resolver.internal.ConsoleRepositoryListener;
import io.airlift.resolver.internal.ConsoleTransferListener;
import io.airlift.resolver.internal.Slf4jLoggerManager;
import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.project.*;
import org.apache.maven.repository.internal.MavenRepositorySystemSession;
import org.apache.maven.repository.internal.MavenServiceLocator;
import org.codehaus.plexus.*;
import org.codehaus.plexus.classworlds.ClassWorld;
import org.codehaus.plexus.logging.Logger;
import org.sonatype.aether.RepositorySystem;
import org.sonatype.aether.artifact.Artifact;
import org.sonatype.aether.collection.CollectRequest;
import org.sonatype.aether.connector.async.AsyncRepositoryConnectorFactory;
import org.sonatype.aether.connector.file.FileRepositoryConnectorFactory;
import org.sonatype.aether.graph.Dependency;
import org.sonatype.aether.graph.Exclusion;
import org.sonatype.aether.impl.internal.SimpleLocalRepositoryManager;
import org.sonatype.aether.repository.LocalRepositoryManager;
import org.sonatype.aether.repository.RemoteRepository;
import org.sonatype.aether.resolution.ArtifactResult;
import org.sonatype.aether.resolution.DependencyRequest;
import org.sonatype.aether.resolution.DependencyResolutionException;
import org.sonatype.aether.resolution.DependencyResult;
import org.sonatype.aether.spi.connector.RepositoryConnectorFactory;
import org.sonatype.aether.util.artifact.DefaultArtifact;
import org.sonatype.aether.util.artifact.JavaScopes;
import org.sonatype.aether.util.filter.DependencyFilterUtils;

import java.io.File;
import java.util.*;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class HttpsArtifactResolver {
    public static final String USER_LOCAL_REPO = "D:\\Module\\Maven\\repo";
    public static final String MAVEN_CENTRAL_URI = "https://repo1.maven.org/maven2/";
    public static final String ALIYUN_MAVEN_CENTRAL_URI = "https://maven.aliyun.com/repository/public";
    public static final Set<String> DEPRECATED_MAVEN_CENTRAL_URIS = ImmutableSet.<String>builder()
            .add("http://repo1.maven.org/maven2")
            .add("http://repo1.maven.org/maven2/")
            .add("http://repo.maven.apache.org/maven2")
            .add("http://repo.maven.apache.org/maven2/").build();

    private final RepositorySystem repositorySystem;
    private final MavenRepositorySystemSession repositorySystemSession;
    private final List<RemoteRepository> repositories;

    public HttpsArtifactResolver(String localRepositoryDir, String... remoteRepositoryUris) {
        this(localRepositoryDir, Arrays.asList(remoteRepositoryUris));
    }

    public HttpsArtifactResolver(String localRepositoryDir, List<String> remoteRepositoryUris) {
        MavenServiceLocator locator = new MavenServiceLocator();
        locator.addService(RepositoryConnectorFactory.class, FileRepositoryConnectorFactory.class);
        locator.addService(RepositoryConnectorFactory.class, AsyncRepositoryConnectorFactory.class);
        repositorySystem = locator.getService(RepositorySystem.class);

        repositorySystemSession = new MavenRepositorySystemSession();

        LocalRepositoryManager localRepositoryManager = new SimpleLocalRepositoryManager(localRepositoryDir);
        repositorySystemSession.setLocalRepositoryManager(localRepositoryManager);

        repositorySystemSession.setTransferListener(new ConsoleTransferListener());
        repositorySystemSession.setRepositoryListener(new ConsoleRepositoryListener());

        List<RemoteRepository> repositories = new ArrayList<>(remoteRepositoryUris.size());
        int index = 0;
        for (String repositoryUri : remoteRepositoryUris) {
            repositories.add(new RemoteRepository("repo-" + index++, "default", repositoryUri));
        }
        this.repositories = Collections.unmodifiableList(repositories);
    }

    public List<Artifact> resolveArtifacts(Artifact... sourceArtifacts) {
        return resolveArtifacts(Arrays.asList(sourceArtifacts));
    }

    public List<Artifact> resolveArtifacts(Iterable<? extends Artifact> sourceArtifacts) {
        CollectRequest collectRequest = new CollectRequest();
        for (Artifact sourceArtifact : sourceArtifacts) {
            collectRequest.addDependency(new Dependency(sourceArtifact, JavaScopes.RUNTIME));
        }
        for (RemoteRepository repository : repositories) {
            // Hack: avoid using deprecated Maven Central URLs
            if (DEPRECATED_MAVEN_CENTRAL_URIS.contains(repository.getUrl())) {
                repository = new RemoteRepository(repository.getId(), repository.getContentType(), MAVEN_CENTRAL_URI);
            }
            collectRequest.addRepository(repository);
        }

        DependencyRequest dependencyRequest = new DependencyRequest(collectRequest, DependencyFilterUtils.classpathFilter(JavaScopes.RUNTIME));

        return resolveArtifacts(dependencyRequest);
    }

    public List<Artifact> resolvePom(File pomFile) {
        if (pomFile == null) {
            throw new RuntimeException("pomFile is null");
        }

        MavenProject pom = getMavenProject(pomFile);
        Artifact rootArtifact = getProjectArtifact(pom);

        CollectRequest collectRequest = new CollectRequest();
        for (org.apache.maven.model.Dependency dependency : pom.getDependencies()) {
            collectRequest.addDependency(toAetherDependency(dependency));
        }

        // Hack: avoid using deprecated Maven Central URLs. The Central Repository no longer supports insecure
        // communication over plain HTTP.
        ImmutableList.Builder<RemoteRepository> allRepositories = ImmutableList.builder();
        for (RemoteRepository repository : pom.getRemoteProjectRepositories()) {
            if (DEPRECATED_MAVEN_CENTRAL_URIS.contains(repository.getUrl())) {
                repository = new RemoteRepository(repository.getId(), repository.getContentType(), MAVEN_CENTRAL_URI);
            }
            allRepositories.add(repository);
        }
        for (RemoteRepository repository : repositories) {
            if (DEPRECATED_MAVEN_CENTRAL_URIS.contains(repository.getUrl())) {
                repository = new RemoteRepository(repository.getId(), repository.getContentType(), MAVEN_CENTRAL_URI);
            }
            allRepositories.add(repository);
        }
        collectRequest.setRepositories(allRepositories.build());

        // Make sure we account for managed dependencies
        if (pom.getDependencyManagement() != null) {
            for (org.apache.maven.model.Dependency managedDependency : pom.getDependencyManagement().getDependencies()) {
                collectRequest.addManagedDependency(toAetherDependency(managedDependency));
            }
        }

        DependencyRequest dependencyRequest = new DependencyRequest(collectRequest, DependencyFilterUtils.classpathFilter(JavaScopes.RUNTIME));
        List<Artifact> artifacts = resolveArtifacts(dependencyRequest);

        Map<String, Artifact> modules = getSiblingModules(pom).stream().collect(toMap(HttpsArtifactResolver::getArtifactKey, identity()));

        return Stream.concat(Stream.of(rootArtifact), artifacts.stream().map(artifact -> modules.getOrDefault(getArtifactKey(artifact), artifact))).collect(toImmutableList());
    }

    private MavenProject getMavenProject(File pomFile) {
        try {
            PlexusContainer container = container();
            org.apache.maven.repository.RepositorySystem lrs = container.lookup(org.apache.maven.repository.RepositorySystem.class);
            ProjectBuilder projectBuilder = container.lookup(ProjectBuilder.class);
            ProjectBuildingRequest request = new DefaultProjectBuildingRequest();
            request.setSystemProperties(requiredSystemProperties());
            request.setRepositorySession(repositorySystemSession);
            request.setProcessPlugins(false);
            ArtifactRepository defaultLocalRepository = lrs.createDefaultLocalRepository();
            defaultLocalRepository.setUrl(USER_LOCAL_REPO);
            request.setLocalRepository(defaultLocalRepository);
            ArtifactRepository defaultRemoteRepository = lrs.createDefaultRemoteRepository();
            defaultRemoteRepository.setUrl(ALIYUN_MAVEN_CENTRAL_URI);
            request.setRemoteRepositories(Arrays.asList(new ArtifactRepository[]{defaultRemoteRepository}.clone()));
            ProjectBuildingResult result = projectBuilder.build(pomFile, request);
            return result.getProject();
        } catch (Exception e) {
            throw new RuntimeException("Error loading pom: " + pomFile.getAbsolutePath(), e);
        }
    }

    private Artifact getProjectArtifact(MavenProject pom) {
        return new DefaultArtifact(pom.getArtifact().getGroupId(), pom.getArtifact().getArtifactId(), pom.getArtifact().getClassifier(), pom.getArtifact().getType(), pom.getArtifact().getVersion(), null, new File(pom.getModel().getBuild().getOutputDirectory()));
    }

    private List<Artifact> getSiblingModules(MavenProject module) {
        if (!module.hasParent() || module.getParentFile() == null) {
            return ImmutableList.of();
        }

        // Parent exists and is a project reactor
        MavenProject parent = module.getParent();
        String parentDir = module.getParentFile().getParent();

        return parent.getModules().stream().map(moduleName -> new File(parentDir, moduleName + "/pom.xml")).filter(File::isFile).map(this::getMavenProject).map(this::getProjectArtifact).collect(toImmutableList());
    }

    /**
     * Returns a string identifying artifact by its maven coordinates.
     */
    private static String getArtifactKey(Artifact artifact) {
        return format("%s:%s:%s:%s", artifact.getGroupId(), artifact.getArtifactId(), artifact.getVersion(), artifact.getClassifier());
    }

    private Properties requiredSystemProperties() {
        Properties properties = new Properties();
        properties.setProperty("java.version", System.getProperty("java.version"));
        return properties;
    }

    private Dependency toAetherDependency(org.apache.maven.model.Dependency dependency) {
        Artifact artifact = new DefaultArtifact(dependency.getGroupId(), dependency.getArtifactId(), dependency.getClassifier(), dependency.getType(), dependency.getVersion());
        ImmutableList.Builder<Exclusion> exclusions = ImmutableList.builder();
        for (org.apache.maven.model.Exclusion exclusion : dependency.getExclusions()) {
            exclusions.add(new Exclusion(exclusion.getGroupId(), exclusion.getArtifactId(), null, "*"));
        }
        return new Dependency(artifact, dependency.getScope(), dependency.isOptional(), exclusions.build());
    }

    private List<Artifact> resolveArtifacts(DependencyRequest dependencyRequest) {
        DependencyResult dependencyResult;
        try {
            dependencyResult = repositorySystem.resolveDependencies(repositorySystemSession, dependencyRequest);
        } catch (DependencyResolutionException e) {
            dependencyResult = e.getResult();
        }
        List<ArtifactResult> artifactResults = dependencyResult.getArtifactResults();
        List<Artifact> artifacts = new ArrayList<>(artifactResults.size());
        for (ArtifactResult artifactResult : artifactResults) {
            if (artifactResult.isMissing()) {
                artifacts.add(artifactResult.getRequest().getArtifact());
            } else {
                artifacts.add(artifactResult.getArtifact());
            }
        }

        return Collections.unmodifiableList(artifacts);
    }

    private static PlexusContainer container() {
        try {
            ClassWorld classWorld = new ClassWorld("plexus.core", Thread.currentThread().getContextClassLoader());

            ContainerConfiguration cc = new DefaultContainerConfiguration().setClassWorld(classWorld).setRealm(null).setName("maven");

            DefaultPlexusContainer container = new DefaultPlexusContainer(cc);

            // NOTE: To avoid inconsistencies, we'll use the Thread context class loader exclusively for lookups
            container.setLookupRealm(null);

            container.setLoggerManager(new Slf4jLoggerManager());
            container.getLoggerManager().setThresholds(Logger.LEVEL_INFO);

            return container;
        } catch (PlexusContainerException e) {
            throw new RuntimeException("Error loading Maven system", e);
        }
    }
}
