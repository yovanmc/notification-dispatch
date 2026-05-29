FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY NotificationDispatch.slnx .
COPY src/NotificationDispatch.Core/NotificationDispatch.Core.csproj src/NotificationDispatch.Core/
COPY src/NotificationDispatch.Api/NotificationDispatch.Api.csproj src/NotificationDispatch.Api/
COPY src/NotificationDispatch.Worker/NotificationDispatch.Worker.csproj src/NotificationDispatch.Worker/
RUN dotnet restore src/NotificationDispatch.Api

COPY src/ src/
RUN dotnet publish src/NotificationDispatch.Api -c Release -o /app /p:ErrorOnDuplicatePublishOutputFiles=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
EXPOSE 8080
ENTRYPOINT ["dotnet", "NotificationDispatch.Api.dll"]
