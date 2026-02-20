import { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import { FolderKanban, ArrowLeft, Calendar, Tag, ChevronRight, AlertCircle, CheckCircle2, Clock } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import { authFetch } from '../api/client';
import { PageHeader, Card, Alert, LoadingState, EmptyState, StatusBadge } from '../components/ui';

interface Project {
  id: string;
  title: string;
  status: 'planning' | 'in-progress' | 'on-hold' | 'completed';
  priority: 'high' | 'medium' | 'low';
  description: string;
  doc_link?: string;
  doc_content?: string;
  created: string;
  updated: string;
  next_steps?: string[];
  tags?: string[];
}

const statusConfig = {
  'planning': { label: 'Planning', variant: 'info' as const, icon: Clock },
  'in-progress': { label: 'In Progress', variant: 'warning' as const, icon: AlertCircle },
  'on-hold': { label: 'On Hold', variant: 'neutral' as const, icon: Clock },
  'completed': { label: 'Completed', variant: 'success' as const, icon: CheckCircle2 },
};

const priorityConfig = {
  'high': { label: 'High', variant: 'danger' as const },
  'medium': { label: 'Medium', variant: 'warning' as const },
  'low': { label: 'Low', variant: 'neutral' as const },
};

function ProjectList() {
  const [projects, setProjects] = useState<Project[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    authFetch('/api/projects')
      .then(res => res.json())
      .then(data => {
        setProjects(data);
        setLoading(false);
      })
      .catch(err => {
        setError(err.message);
        setLoading(false);
      });
  }, []);

  if (loading) {
    return <LoadingState message="Loading projects..." />;
  }

  if (error) {
    return <Alert variant="error" title="Error loading projects">{error}</Alert>;
  }

  return (
    <div className="space-y-6">
      <PageHeader icon={FolderKanban} title="Projects" subtitle="Development projects and features to be revisited. Click on a project to view details.">
        <span className="text-xs text-gray-500">{projects.length} project(s)</span>
      </PageHeader>

      {projects.length === 0 ? (
        <EmptyState
          icon={FolderKanban}
          title="No projects yet"
          message="Projects will appear here when created."
        />
      ) : (
        <div className="grid gap-4">
          {projects.map(project => {
            const status = statusConfig[project.status];
            const priority = priorityConfig[project.priority];

            return (
              <Link
                key={project.id}
                to={`/system/projects/${project.id}`}
                className="bg-white border border-gray-200 rounded-xl shadow-sm p-4 hover:shadow-md transition-shadow"
              >
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <div className="flex items-center gap-3 mb-2">
                      <h2 className="text-base font-semibold text-gray-900">{project.title}</h2>
                      <StatusBadge variant={status.variant}>{status.label}</StatusBadge>
                      <StatusBadge variant={priority.variant}>{priority.label}</StatusBadge>
                    </div>
                    <p className="text-sm text-gray-600 mb-3">{project.description}</p>
                    <div className="flex items-center gap-4 text-xs text-gray-500">
                      <span className="flex items-center gap-1">
                        <Calendar className="h-3 w-3" />
                        Updated: {project.updated}
                      </span>
                      {project.tags && project.tags.length > 0 && (
                        <span className="flex items-center gap-1">
                          <Tag className="h-3 w-3" />
                          {project.tags.join(', ')}
                        </span>
                      )}
                    </div>
                  </div>
                  <ChevronRight className="h-5 w-5 text-gray-400" />
                </div>
              </Link>
            );
          })}
        </div>
      )}
    </div>
  );
}

function ProjectDetail({ projectId }: { projectId: string }) {
  const [project, setProject] = useState<Project | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    authFetch(`/api/projects/${projectId}`)
      .then(res => {
        if (!res.ok) throw new Error('Project not found');
        return res.json();
      })
      .then(data => {
        setProject(data);
        setLoading(false);
      })
      .catch(err => {
        setError(err.message);
        setLoading(false);
      });
  }, [projectId]);

  if (loading) {
    return <LoadingState message="Loading project..." />;
  }

  if (error || !project) {
    return (
      <div className="space-y-4">
        <Link to="/system/projects" className="inline-flex items-center gap-2 text-blue-600 hover:text-blue-800">
          <ArrowLeft className="h-4 w-4" />
          Back to Projects
        </Link>
        <Alert variant="error" title="Error">{error || 'Project not found'}</Alert>
      </div>
    );
  }

  const status = statusConfig[project.status];
  const priority = priorityConfig[project.priority];

  return (
    <div className="space-y-6">
      <Link to="/system/projects" className="inline-flex items-center gap-2 text-blue-600 hover:text-blue-800">
        <ArrowLeft className="h-4 w-4" />
        Back to Projects
      </Link>

      <Card>
        <div className="flex items-start justify-between mb-4">
          <div>
            <h1 className="text-xl font-bold text-gray-900 mb-2">{project.title}</h1>
            <div className="flex items-center gap-3">
              <StatusBadge variant={status.variant}>{status.label}</StatusBadge>
              <StatusBadge variant={priority.variant}>{priority.label} Priority</StatusBadge>
            </div>
          </div>
          <div className="text-right text-xs text-gray-500">
            <p>Created: {project.created}</p>
            <p>Updated: {project.updated}</p>
          </div>
        </div>

        <p className="text-sm text-gray-700 mb-4">{project.description}</p>

        {project.tags && project.tags.length > 0 && (
          <div className="flex items-center gap-2 mb-4">
            <Tag className="h-4 w-4 text-gray-400" />
            {project.tags.map(tag => (
              <span key={tag} className="px-2 py-0.5 bg-gray-100 text-gray-600 rounded text-xs">
                {tag}
              </span>
            ))}
          </div>
        )}

        {project.next_steps && project.next_steps.length > 0 && (
          <div className="mt-6">
            <h3 className="text-base font-semibold text-gray-900 mb-3">Next Steps</h3>
            <ul className="space-y-2">
              {project.next_steps.map((step, i) => (
                <li key={i} className="flex items-start gap-2">
                  <span className="flex-shrink-0 w-5 h-5 bg-blue-100 text-blue-700 rounded-full text-xs flex items-center justify-center mt-0.5">
                    {i + 1}
                  </span>
                  <span className="text-sm text-gray-700">{step}</span>
                </li>
              ))}
            </ul>
          </div>
        )}
      </Card>

      {project.doc_content && (
        <Card title="Documentation">
          <div className="prose prose-sm max-w-none">
            <ReactMarkdown>{project.doc_content}</ReactMarkdown>
          </div>
        </Card>
      )}
    </div>
  );
}

export function Projects() {
  const { projectId } = useParams<{ projectId?: string }>();

  if (projectId) {
    return <ProjectDetail projectId={projectId} />;
  }

  return <ProjectList />;
}
