import { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import { FolderKanban, ArrowLeft, Calendar, Tag, ChevronRight, AlertCircle, CheckCircle2, Clock } from 'lucide-react';
import ReactMarkdown from 'react-markdown';

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
  'planning': { label: 'Planning', color: 'bg-blue-100 text-blue-800', icon: Clock },
  'in-progress': { label: 'In Progress', color: 'bg-yellow-100 text-yellow-800', icon: AlertCircle },
  'on-hold': { label: 'On Hold', color: 'bg-gray-100 text-gray-800', icon: Clock },
  'completed': { label: 'Completed', color: 'bg-green-100 text-green-800', icon: CheckCircle2 },
};

const priorityConfig = {
  'high': { label: 'High', color: 'bg-red-100 text-red-800' },
  'medium': { label: 'Medium', color: 'bg-orange-100 text-orange-800' },
  'low': { label: 'Low', color: 'bg-gray-100 text-gray-600' },
};

function ProjectList() {
  const [projects, setProjects] = useState<Project[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch('/api/projects')
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
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-4">
        <p className="text-red-700">Error loading projects: {error}</p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <FolderKanban className="h-8 w-8 text-blue-600" />
          <h1 className="text-2xl font-bold text-gray-900">Projects</h1>
        </div>
        <span className="text-sm text-gray-500">{projects.length} project(s)</span>
      </div>

      <p className="text-gray-600">
        Development projects and features to be revisited. Click on a project to view details.
      </p>

      {projects.length === 0 ? (
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-8 text-center">
          <FolderKanban className="h-12 w-12 text-gray-400 mx-auto mb-4" />
          <p className="text-gray-600">No projects yet.</p>
        </div>
      ) : (
        <div className="grid gap-4">
          {projects.map(project => {
            const status = statusConfig[project.status];
            const priority = priorityConfig[project.priority];
            const StatusIcon = status.icon;

            return (
              <Link
                key={project.id}
                to={`/system/projects/${project.id}`}
                className="bg-white border border-gray-200 rounded-lg p-4 hover:shadow-md transition-shadow"
              >
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <div className="flex items-center gap-3 mb-2">
                      <h2 className="text-lg font-semibold text-gray-900">{project.title}</h2>
                      <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${status.color}`}>
                        <StatusIcon className="h-3 w-3" />
                        {status.label}
                      </span>
                      <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${priority.color}`}>
                        {priority.label}
                      </span>
                    </div>
                    <p className="text-gray-600 text-sm mb-3">{project.description}</p>
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
    fetch(`/api/projects/${projectId}`)
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
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  if (error || !project) {
    return (
      <div className="space-y-4">
        <Link to="/system/projects" className="inline-flex items-center gap-2 text-blue-600 hover:text-blue-800">
          <ArrowLeft className="h-4 w-4" />
          Back to Projects
        </Link>
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <p className="text-red-700">{error || 'Project not found'}</p>
        </div>
      </div>
    );
  }

  const status = statusConfig[project.status];
  const priority = priorityConfig[project.priority];
  const StatusIcon = status.icon;

  return (
    <div className="space-y-6">
      <Link to="/system/projects" className="inline-flex items-center gap-2 text-blue-600 hover:text-blue-800">
        <ArrowLeft className="h-4 w-4" />
        Back to Projects
      </Link>

      <div className="bg-white border border-gray-200 rounded-lg p-6">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h1 className="text-2xl font-bold text-gray-900 mb-2">{project.title}</h1>
            <div className="flex items-center gap-3">
              <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${status.color}`}>
                <StatusIcon className="h-3 w-3" />
                {status.label}
              </span>
              <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${priority.color}`}>
                {priority.label} Priority
              </span>
            </div>
          </div>
          <div className="text-right text-sm text-gray-500">
            <p>Created: {project.created}</p>
            <p>Updated: {project.updated}</p>
          </div>
        </div>

        <p className="text-gray-700 mb-4">{project.description}</p>

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
            <h3 className="text-lg font-semibold text-gray-900 mb-3">Next Steps</h3>
            <ul className="space-y-2">
              {project.next_steps.map((step, i) => (
                <li key={i} className="flex items-start gap-2">
                  <span className="flex-shrink-0 w-5 h-5 bg-blue-100 text-blue-700 rounded-full text-xs flex items-center justify-center mt-0.5">
                    {i + 1}
                  </span>
                  <span className="text-gray-700">{step}</span>
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>

      {project.doc_content && (
        <div className="bg-white border border-gray-200 rounded-lg p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Documentation</h3>
          <div className="prose prose-sm max-w-none">
            <ReactMarkdown>{project.doc_content}</ReactMarkdown>
          </div>
        </div>
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
