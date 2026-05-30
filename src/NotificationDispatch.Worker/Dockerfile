FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY NotificationDispatch.slnx .
COPY src/NotificationDispatch.Core/NotificationDispatch.Core.csproj src/NotificationDispatch.Core/
COPY src/NotificationDispatch.Infrastructure/NotificationDispatch.Infrastructure.csproj src/NotificationDispatch.Infrastructure/
COPY src/NotificationDispatch.Worker/NotificationDispatch.Worker.csproj src/NotificationDispatch.Worker/
RUN dotnet restore src/NotificationDispatch.Worker

COPY src/ src/
RUN dotnet publish src/NotificationDispatch.Worker -c Release -o /app

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app .
USER app
ENTRYPOINT ["dotnet", "NotificationDispatch.Worker.dll"]
