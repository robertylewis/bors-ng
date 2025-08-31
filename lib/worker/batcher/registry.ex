defmodule BorsNG.Worker.Batcher.Registry do
  @moduledoc """
  The "Batcher" manages the backlog of batches that each project has.
  This is the registry of each individual batcher.
  It starts the batcher if it doesn't exist,
  restarts it if it crashes,
  and logs the crashes because that's needed sometimes.

  Note that the batcher and registry are always on the same node.
  Sharding between them will be done by directing which registry to go to.
  """

  use GenServer

  alias BorsNG.Worker.Batcher
  alias BorsNG.Worker.Zulip
  alias BorsNG.Database.Batch
  alias BorsNG.Database.Crash
  alias BorsNG.Database.Project
  alias BorsNG.Database.Repo

  require Logger

  @name BorsNG.Worker.Batcher.Registry

  # Public API

  def start_link do
    GenServer.start_link(__MODULE__, :ok, name: @name)
  end

  def get(project_id, count \\ 0)

  def get(project_id, 5) when is_integer(project_id) do
    pid = GenServer.call(@name, {:get, project_id})
    # process haven't been started at all
    if pid == nil do
      do_start(project_id)
    end
  end

  def get(project_id, count) when is_integer(project_id) do
    pid = GenServer.call(@name, {:get, project_id})

    if pid == nil do
      # broadcasted message from Batcher to registry hasn't been processed
      # we give chance them to register before trying to create a new one
      Process.sleep(100)
      get(project_id, count + 1)
    else
      pid
    end
  end

  def monitor(pid, project_id) do
    GenServer.cast(@name, {:monitor, project_id, pid})
  end

  # Server callbacks

  def init(:ok) do
    Project.active()
    |> Repo.all()
    |> Enum.map(fn %{id: id} -> id end)
    |> Enum.uniq()
    |> Enum.map(&{&1, do_start(&1)})

    # When the worker actually started, they'll register them self to the
    # registry using monitor.
    {:ok, {Map.new(), Map.new()}}
  end

  def do_start(project_id) do
    {:ok, pid} = Batcher.Supervisor.start(project_id)
    pid
  end

  def handle_call({:get, project_id}, _from, {names, _refs} = state) do
    {pid, state} =
      case names[project_id] do
        nil ->
          {nil, state}

        pid ->
          {pid, state}
      end

    {:reply, pid, state}
  end

  def handle_cast({:monitor, project_id, pid}, {names, refs} = state) do
    new_state =
      case names[project_id] do
        nil ->
          ref = Process.monitor(pid)
          names = Map.put(names, project_id, pid)
          refs = Map.put(refs, ref, project_id)
          {names, refs}

        pid ->
          Logger.warn(
            "Project #{inspect(project_id)} already monitored #{inspect(pid)} by #{inspect(names[project_id])}"
          )

          state
      end

    {:noreply, new_state}
  end

  def handle_info({:DOWN, ref, :process, pid, :normal}, {names, refs}) do
    {project_id, refs} = Map.pop(refs, ref)

    names =
      if names[project_id] == pid do
        Map.delete(names, project_id)
      else
        names
      end

    {:noreply, {names, refs}}
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, {names, refs}) do
    Logger.warn(
      "Batcher #{inspect(pid)} for project #{inspect(refs[ref])} crashed with state #{inspect({names, refs})}"
    )

    {project_id, refs} = Map.pop(refs, ref)

    names =
      if names[project_id] == pid do
        Map.delete(names, project_id)
      else
        names
      end

    crash_message = try do
      build_message(project_id, pid, reason)
    rescue
      e ->
        e_message = "Failed to build crash message:\n#{inspect(e, pretty: true, width: 60)}"
        Logger.error(e_message)
        "#{e_message}\n\nCrash reason:\n#{inspect(reason, pretty: true, width: 60)}"
    end

    Zulip.send_message("🚨 bors batch worker crashed!\n\n" <> crash_message)

    project_id
    |> Batch.all_for_project(:waiting)
    |> Repo.all()
    |> Enum.each(&Repo.delete!/1)

    project_id
    |> Batch.all_for_project(:running)
    |> Repo.all()
    |> Enum.map(&Batch.changeset(&1, %{state: :canceled}))
    |> Enum.each(&Repo.update!/1)

    Repo.insert(%Crash{
      project_id: project_id,
      component: "batch",
      crash: crash_message
    })

    {:noreply, {names, refs}}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp build_message(project_id, pid, reason) do
    waiting = project_id
    |> Batch.all_for_project(:waiting)
    |> Repo.all()
    |> Repo.preload([:patches])

    project = Repo.get(Project, project_id)
    project_pr_url = Confex.fetch_env!(:bors, :html_github_root) <> "/" <> project.name <> "/pull/"

    waiting_message = if length(waiting) > 0 do
      """
      The following batches were "Waiting" and will now be deleted:
      #{waiting
      |> batch_prs
      |> Enum.map(fn {id, prs} ->
        """
        - Batch #{id}:
        #{prs |> Enum.map(& "  - [##{&1}](#{project_pr_url}#{&1})") |> Enum.join("\n")}
        """
      end)}
      """
    else
      ""
    end

    running = project_id
    |> Batch.all_for_project(:running)
    |> Repo.all()
    |> Repo.preload([:patches])

    running_message = if length(running) > 0 do
      """
      The following batches were "Running" and will now be canceled:
      #{running
      |> batch_prs
      |> Enum.map(fn {id, prs} ->
        """
        - Batch #{id}:
        #{prs |> Enum.map(& "  - [##{&1}](#{project_pr_url}#{&1})") |> Enum.join("\n")}
        """
      end)}
      """
    else
      ""
    end

    """
    Project: `#{project.name}`
    Batcher Process ID: `#{pid}`
    Reason:
    ```
    #{inspect(reason, pretty: true, width: 60)}
    ```

    #{waiting_message}

    #{running_message}
    """
  end
  # Given a list of batches (with preload [:patches]),
  # return a list of {batch.id, [list of patches in the batch]}
  defp batch_prs(batches) do
    Enum.map(batches, fn batch ->
      pr_xrefs = batch.patches
      |> Enum.map(& &1.pr_xref)

      {batch.id, pr_xrefs}
    end)
  end
end
