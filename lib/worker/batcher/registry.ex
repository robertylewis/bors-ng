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

    send_zulip_notification(project_id, pid, reason)

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
      crash: inspect(reason, pretty: true, width: 60)
    })

    {:noreply, {names, refs}}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp send_zulip_notification(project_id, pid, reason) do
    try do
      zulip_api_url = Confex.fetch_env!(:bors, :zulip_api_url)
      bot_email = Confex.fetch_env!(:bors, :zulip_bot_email)
      bot_api_key = Confex.fetch_env!(:bors, :zulip_bot_api_key)
      channel_name = Confex.fetch_env!(:bors, :zulip_channel_name)
      topic = Confex.fetch_env!(:bors, :zulip_topic)

      # Skip if any required config is empty
      if empty_string?(zulip_api_url) or empty_string?(bot_email) or empty_string?(bot_api_key) do
        :ok
      else
        message = build_message(project_id, pid, reason)

        body = URI.encode_query(%{
          "type" => "channel",
          "to" => channel_name,
          "topic" => topic,
          "content" => message
        })

        client = Tesla.client([
          {Tesla.Middleware.BasicAuth, username: bot_email, password: bot_api_key},
          Tesla.Middleware.FormUrlencoded
        ], Tesla.Adapter.Hackney)

        # Fire and forget - don't block on response
        Task.start(fn ->
          Tesla.post(client, zulip_api_url <> "messages", body)
        end)
      end
    rescue
      e ->
        Logger.error("Failed to send Zulip notification: #{inspect(e)}")
        :error
    end
  end
  defp empty_string?(""), do: true
  defp empty_string?(_), do: false

  defp build_message(project_id, pid, reason) do
    waiting = project_id
    |> Batch.all_for_project(:waiting)
    |> Repo.all()
    |> Repo.preload([patches: :patch])

    waiting_message = if length(waiting) > 0 do
      """
      The following batches were "Waiting" and will now be deleted:
      #{waiting
      |> batch_prs
      |> Enum.map(fn {id, prs} ->
        """
        - Batch #{id}:
        #{prs |> Enum.map(& "  - ##{&1}") |> Enum.join("\n")}
        """
      end)}
      """
    else
      ""
    end

    running = project_id
    |> Batch.all_for_project(:running)
    |> Repo.all()
    |> Repo.preload([patches: :patch])

    running_message = if length(running) > 0 do
      """
      The following batches were "Running" and will now be canceled:
      #{running
      |> batch_prs
      |> Enum.map(fn {id, prs} ->
        """
        - Batch #{id}:
        #{prs |> Enum.map(& "  - ##{&1}") |> Enum.join("\n")}
        """
      end)}
      """
    else
      ""
    end

    """
    🚨 bors batch worker crashed!

    Project: `#{project_id}`
    PID: `#{inspect(pid)}`
    Reason:
    ```
    #{inspect(reason, pretty: true, width: 60)}
    ```

    #{waiting_message}

    #{running_message}
    """
  end
  # Get list of patches a set of batches
  defp batch_prs(batches) do
    Enum.map(batches, fn batch ->
      pr_xrefs = batch.patches
      |> Enum.map(& &1.patch.pr_xref)

      {batch.id, pr_xrefs}
    end)
  end
end
